package stream

import (
	"context"
	"errors"
	"fmt"
)

// ErrDataCorrupt 表示输入事件本身损坏（与超范围迟到区分）。
var ErrDataCorrupt = errors.New("data corrupt")

// Runtime 编排：可重放源 -> 去重 -> 水位线/窗口 -> 有界 Sink，并负责检查点与恢复。
type Runtime struct {
	src   Source
	wm    *WatermarkTracker
	win   *WindowManager
	dedup *Deduper
	sink  *Sink
	store *Store

	nextSeq   int64
	retention int64
	processed int64 // 自上次回收以来处理的事件数，用于周期性触发回收

	// history 已对外确认的结果（按 Seq 有序），供恢复时回退判断与向空下游重发。
	history []Result

	// Duplicates 记录所有被识别为重复投递的事件（与数据错误、超范围迟到分开统计）。
	Duplicates []DuplicateRecord
	// LateDropped 记录所有被丢弃的超范围迟到事件。
	LateDropped []LateRecord
	// DataErrors 记录所有数据错误（与迟到丢弃、重复区分）。
	DataErrors []error
}

// New 构造运行时。windowSize 为窗口宽度，allowedLateness 为允许迟到范围（毫秒），
// retention 为去重身份与结果历史的事件时间保留范围（毫秒，建议 >= allowedLateness）。
func New(src Source, windowSize, allowedLateness, retention int64, sink *Sink, store *Store) *Runtime {
	return &Runtime{
		src:       src,
		wm:        NewWatermarkTracker(allowedLateness),
		win:       NewWindowManager(windowSize),
		dedup:     NewDeduper(retention),
		sink:      sink,
		store:     store,
		retention: retention,
	}
}

// validate 校验事件数据合法性。
func validate(ev Event) error {
	if ev.ID == "" {
		return fmt.Errorf("%w: empty id", ErrDataCorrupt)
	}
	if ev.Key == "" {
		return fmt.Errorf("%w: empty key", ErrDataCorrupt)
	}
	return nil
}

// pruneInterval 处理多少条事件后做一次保留范围回收，限制状态随输入条数的增长上界。
const pruneInterval = 64

// Step 处理一条输入事件；返回 done=true 表示输入流已耗尽。
// Sink 缓冲满时写阻塞，输入读取自然被节制（背压），内存有界。
func (r *Runtime) Step(ctx context.Context) (done bool, err error) {
	ev, ok, err := r.src.Next()
	if err != nil {
		return false, err
	}
	if !ok {
		return true, nil
	}
	if err := validate(ev); err != nil {
		r.DataErrors = append(r.DataErrors, err)
		return false, nil
	}
	// 重复投递在结果层面幂等：重复事件不推进水位线、不归并窗口，只单独统计，
	// 与数据错误、超范围迟到明确分开。保留范围内的重复（任意顺序混入）都在此被识别。
	if r.dedup.Seen(ev) {
		r.Duplicates = append(r.Duplicates, DuplicateRecord{Event: ev})
		return false, nil
	}
	watermark := r.wm.Watermark()
	if r.wm.IsLate(ev) && r.win.IsClosed(ev, watermark) {
		start := r.win.WindowStart(ev.EventTime)
		r.LateDropped = append(r.LateDropped, LateRecord{
			Event:       ev,
			WindowStart: start,
			WindowEnd:   start + r.win.size,
			Watermark:   watermark,
			Reason:      "window already closed",
		})
		return false, nil
	}
	// 在允许范围内的迟到事件按确定性规则归并（与正常事件同一 Add 路径）。
	r.wm.Observe(ev)
	r.dedup.Mark(ev)
	r.processed++
	r.win.Add(ev)
	if r.processed%pruneInterval == 0 {
		r.prune()
	}
	results, next := r.win.CloseUpTo(r.wm.Watermark(), r.nextSeq)
	r.recordResults(results)
	r.nextSeq = next
	for _, res := range results {
		if err := r.sink.Write(ctx, res); err != nil {
			return false, err
		}
	}
	return false, nil
}

// Flush 在输入结束后关闭所有剩余窗口并产出结果。
func (r *Runtime) Flush(ctx context.Context) error {
	results, next := r.win.CloseUpTo(1<<62, r.nextSeq)
	r.recordResults(results)
	r.nextSeq = next
	for _, res := range results {
		if err := r.sink.Write(ctx, res); err != nil {
			return err
		}
	}
	return nil
}

// Run 持续消费输入直到耗尽，最后 Flush 并做一次最终检查点。
func (r *Runtime) Run(ctx context.Context) error {
	for {
		done, err := r.Step(ctx)
		if err != nil {
			return err
		}
		if done {
			break
		}
	}
	if err := r.Flush(ctx); err != nil {
		return err
	}
	return r.Checkpoint()
}

// Checkpoint 生成并持久化检查点，同时确认已产出的结果对外可见。
func (r *Runtime) Checkpoint() error {
	cp := Checkpoint{
		Version:      CheckpointVersion,
		SourceOffset: r.src.Offset(),
		MaxEventTime: r.wm.maxSeen,
		Started:      r.wm.started,
		NextSeq:      r.nextSeq,
		Windows:      toSnapshot(r.win.snapshot()),
		Dedup:        r.dedup.snapshot(),
		Results:      r.snapshotHistory(),
	}
	if err := r.store.Save(cp); err != nil {
		return err
	}
	// 推进独立于下游的已确认进度高水位（只进不退），供后续跨进程恢复识别回退。
	if err := r.store.Commit(r.nextSeq); err != nil {
		return err
	}
	r.sink.ConfirmUpTo(r.nextSeq - 1)
	return nil
}

// Restore 从最近检查点恢复到一个（可能全新的）进程与（可能为空的）下游。
// 拒绝原因可区分，绝不带病从中途静默继续：
//   - ErrCheckpointCorrupt / ErrCheckpointVersion：由 Store.Load 在校验阶段给出；
//   - ErrCheckpointCorrupt：检查点内部结构不自洽（序号与窗口/历史对不上）；
//   - ErrOffsetRegression：恢复进度会覆盖下游已经看过的结果，或源位点越界。
//
// 下游为空（全新展示端）时，从可重放输入的起点确定性地全量重放，让所有历史结果
// 重新变得可见，再由后续输入无缝接上；下游非空时从检查点位点续跑，未确认结果重发。
func (r *Runtime) Restore(ctx context.Context) error {
	cp, err := r.store.Load()
	if err != nil {
		return err // ErrCheckpointCorrupt / ErrCheckpointVersion / ErrNoCheckpoint
	}
	if err := validateCheckpoint(cp); err != nil {
		return err
	}
	// 进度回退（跨进程）：检查点落后于磁盘上“只进不退”的已确认进度高水位，
	// 即使下游是全新空实例也拒绝——恢复到更旧进度会把已对外可见的结果覆盖/倒退。
	committed, err := r.store.LoadCommit()
	if err != nil && !errors.Is(err, ErrNoCheckpoint) {
		return err
	}
	hasCommit := err == nil
	if hasCommit && cp.NextSeq < committed {
		return ErrOffsetRegression
	}
	// 进度回退（下游侧）：检查点已确认输出落后于当前下游已确认输出，恢复会倒退可见结果。
	if cp.NextSeq-1 < r.sink.ConfirmedSeq() {
		return ErrOffsetRegression
	}

	// 全新空下游：用可重放输入从头全量重放，历史结果按确定顺序重新可见，
	// 内存由有界 Sink 的背压兜底（空/慢下游持续不消费也不会被撑爆）。
	if r.sink.ConfirmedSeq() < 0 && r.sink.Pending() == 0 {
		return r.replayFromScratch(ctx, cp)
	}

	// 同一（非空）下游续跑：源位点回退重放、窗口/去重状态还原；
	// 未确认输出截断为未发生，由重放确定性地重新产出，避免重复投递。
	if err := r.src.SeekOffset(cp.SourceOffset); err != nil {
		return err
	}
	r.wm.maxSeen = cp.MaxEventTime
	r.wm.started = cp.Started
	r.nextSeq = cp.NextSeq
	r.win.restore(fromSnapshot(cp.Windows))
	r.dedup.restore(cp.Dedup)
	r.history = append([]Result(nil), cp.Results...)
	r.sink.TruncateUnconfirmed()
	return nil
}

// replayFromScratch 面向全新进程 + 空下游：不套用检查点的中间状态，而是清空运行状态，
// 把源拨回起点，重放确定性地产出与“从头不中断跑一遍”逐条一致的完整结果。
// 检查点仅作为“确实存在一次已确认运行”的存在性与代际凭证。
func (r *Runtime) replayFromScratch(ctx context.Context, cp Checkpoint) error {
	if err := r.src.SeekOffset(0); err != nil {
		return err
	}
	r.wm = NewWatermarkTracker(r.wm.allowedLateness)
	r.win = NewWindowManager(r.win.size)
	r.dedup = NewDeduper(r.retention)
	r.nextSeq = 0
	r.processed = 0
	r.history = nil
	r.LateDropped = nil
	r.Duplicates = nil
	r.DataErrors = nil
	if err := r.runToEnd(ctx); err != nil {
		return err
	}
	// 重放完成后，结果与位点必须与已落盘检查点兼容（不能落到比检查点更旧的进度）。
	if r.nextSeq < cp.NextSeq {
		return ErrOffsetRegression
	}
	return r.Checkpoint()
}

// runToEnd 消费输入到耗尽并 Flush（不做检查点）。
func (r *Runtime) runToEnd(ctx context.Context) error {
	for {
		done, err := r.Step(ctx)
		if err != nil {
			return err
		}
		if done {
			break
		}
	}
	return r.Flush(ctx)
}

// validateCheckpoint 校验检查点内部结构自洽；不自洽按损坏处理，与版本不兼容区分。
func validateCheckpoint(cp Checkpoint) error {
	if cp.SourceOffset < 0 || cp.NextSeq < 0 {
		return ErrCheckpointCorrupt
	}
	if len(cp.Results) > 0 {
		last := cp.Results[len(cp.Results)-1]
		// 历史尾部的最后一条必须等于已确认输出的最高序号。
		if last.Seq != cp.NextSeq-1 {
			return ErrCheckpointCorrupt
		}
		// 保留尾部内部序号必须连续，否则内容被破坏。
		for i := 1; i < len(cp.Results); i++ {
			if cp.Results[i].Seq != cp.Results[i-1].Seq+1 {
				return ErrCheckpointCorrupt
			}
		}
	}
	return nil
}

// recordResults 把新产出的结果追加到历史（按 Seq 有序），并按保留范围裁剪。
func (r *Runtime) recordResults(results []Result) {
	if len(results) == 0 {
		return
	}
	r.history = append(r.history, results...)
	r.trimHistory()
}

// snapshotHistory 返回结果历史的拷贝（已经按保留范围裁剪为尾部）。
func (r *Runtime) snapshotHistory() []Result {
	if len(r.history) == 0 {
		return nil
	}
	out := make([]Result, len(r.history))
	copy(out, r.history)
	return out
}

// pruneHorizon 返回当前事件时间回收下界。
func (r *Runtime) pruneHorizon() int64 {
	if !r.wm.started {
		return 1<<62 - 1 // 尚无事件时间时不回收
	}
	return r.wm.maxSeen - r.retention
}

// prune 按保留范围回收去重身份与结果历史：落在 horizon 内的身份照常去重，
// 之外的窗口早已关闭、结果不会再被改变，可安全丢弃，状态容量因此有界。
func (r *Runtime) prune() {
	horizon := r.pruneHorizon()
	r.dedup.Prune(horizon)
	r.trimHistory()
}

// trimHistory 丢弃窗口结束时间早于回收下界的历史结果，但至少保留最后一条，
// 使恢复时的“进度回退”判断始终有参照。
func (r *Runtime) trimHistory() {
	if len(r.history) == 0 || !r.wm.started {
		return
	}
	horizon := r.pruneHorizon()
	cut := 0
	for cut < len(r.history) && r.history[cut].WindowEnd < horizon {
		cut++
	}
	if cut >= len(r.history) {
		cut = len(r.history) - 1 // 永远保留最新一条作为回退判断的高水位
	}
	if cut > 0 {
		r.history = append([]Result(nil), r.history[cut:]...)
	}
}

// NextSeq 返回下一条输出结果的序号（测试用于观察恢复位点）。
func (r *Runtime) NextSeq() int64 { return r.nextSeq }

// DedupCount 返回保留范围内的去重身份条目数（测试用于验证状态有界）。
func (r *Runtime) DedupCount() int { return r.dedup.Count() }

// HistoryCount 返回当前保留的已确认结果历史条数（测试用于验证状态有界）。
func (r *Runtime) HistoryCount() int { return len(r.history) }

// Watermark 返回当前水位线。
func (r *Runtime) Watermark() int64 { return r.wm.Watermark() }

func toSnapshot(m map[int64]map[string]windowState) map[int64]map[string]WindowState {
	out := map[int64]map[string]WindowState{}
	for start, keys := range m {
		inner := map[string]WindowState{}
		for k, st := range keys {
			inner[k] = WindowState{Count: st.count, Sum: st.sum}
		}
		out[start] = inner
	}
	return out
}

func fromSnapshot(m map[int64]map[string]WindowState) map[int64]map[string]windowState {
	out := map[int64]map[string]windowState{}
	for start, keys := range m {
		inner := map[string]windowState{}
		for k, st := range keys {
			inner[k] = windowState{count: st.Count, sum: st.Sum}
		}
		out[start] = inner
	}
	return out
}
