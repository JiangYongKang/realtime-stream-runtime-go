package stream

import (
	"context"
	"errors"
	"fmt"
)

// ErrDataCorrupt 表示输入事件本身损坏（与超范围迟到、重复投递区分）。
var ErrDataCorrupt = errors.New("data corrupt")

// Config 是运行时的扩展配置。
type Config struct {
	WindowSize      int64
	AllowedLateness int64
	// DedupRetention 去重状态的事件时间保留范围（毫秒）：
	// watermark-eventTime >= DedupRetention 的已见 ID 会被回收；
	// 0 表示不做回收。落在保留范围内的重复事件仍然被幂等去重。
	DedupRetention int64
	// Generation 本次运行的代际标识（lineage）。恢复时检查点的代际必须与之一致，
	// 否则以 ErrGenerationIncompatible 明确拒绝，防止跨逻辑代际带病续跑。
	Generation string
}

// Runtime 编排：可重放源 -> 去重/水位线/窗口 -> 有界 Sink，并负责检查点与恢复。
type Runtime struct {
	cfg   Config
	src   Source
	wm    *WatermarkTracker
	win   *WindowManager
	dedup *DedupTracker
	sink  *Sink
	store *Store

	nextSeq int64

	// LateDropped 记录所有被丢弃的超范围迟到事件。
	LateDropped []LateRecord
	// DataErrors 记录所有数据错误（与迟到丢弃、重复投递区分）。
	DataErrors []error
	// Duplicates 记录所有被识别出的重复投递（按 ID 幂等，不影响聚合）。
	Duplicates []DupRecord
	// DedupEvictions 记录去重状态累计回收条数（有界状态的可观察量）。
	DedupEvictions int
}

// New 以默认配置构造运行时（无去重回收、无代际），保持与旧用法兼容。
// windowSize 为窗口宽度，allowedLateness 为允许迟到范围（毫秒）。
func New(src Source, windowSize, allowedLateness int64, sink *Sink, store *Store) *Runtime {
	return NewWithConfig(src, Config{WindowSize: windowSize, AllowedLateness: allowedLateness}, sink, store)
}

// NewWithConfig 按扩展配置构造运行时。
func NewWithConfig(src Source, cfg Config, sink *Sink, store *Store) *Runtime {
	return &Runtime{
		cfg:   cfg,
		src:   src,
		wm:    NewWatermarkTracker(cfg.AllowedLateness),
		win:   NewWindowManager(cfg.WindowSize),
		dedup: NewDedupTracker(cfg.DedupRetention),
		sink:  sink,
		store: store,
	}
}

// validate 校验事件数据合法性。
func validate(ev Event) error {
	if ev.Key == "" {
		return fmt.Errorf("%w: empty key", ErrDataCorrupt)
	}
	return nil
}

// Step 处理一条输入事件；返回 done=true 表示输入流已耗尽。
// Sink 缓冲满时写阻塞，输入读取自然被节制（背压），内存有界。
// 重复事件不推进水位线、不进入窗口，任何混入顺序下聚合都与无重复输入一致。
func (r *Runtime) Step(ctx context.Context) (done bool, err error) {
	ev, ok, err := r.src.Next()
	if err != nil {
		return false, err
	}
	if !ok {
		return true, nil
	}
	// 先做只读身份识别：重复/冲突与数据错误、超范围迟到分开统计。
	// 登记（Commit）推迟到事件确定被吸收之后，从而保证检查点保存的已见集合
	// 恰好只覆盖已消费位点，崩溃重放未检查点的事件不会被误判成重复。
	if ev.ID != "" {
		switch r.dedup.Classify(ev) {
		case dedupDuplicate:
			r.Duplicates = append(r.Duplicates, DupRecord{
				Event:     ev,
				Watermark: r.wm.Watermark(),
				Reason:    "duplicate delivery: id already seen with identical payload",
			})
			return false, nil
		case dedupConflict:
			r.DataErrors = append(r.DataErrors, fmt.Errorf(
				"%w: id %q replayed with conflicting payload (key=%q time=%d value=%d)",
				ErrDataCorrupt, ev.ID, ev.Key, ev.EventTime, ev.Value))
			return false, nil
		}
	}
	if err := validate(ev); err != nil {
		r.DataErrors = append(r.DataErrors, err)
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
	// 新事件确定被吸收：此时才登记 ID。重复事件以任意顺序/次数混入，
	// 在允许范围内迟到的重复也不会二次计入，聚合结果与无重复输入逐条一致。
	if ev.ID != "" {
		r.dedup.Commit(ev)
	}
	// 在允许范围内的迟到事件按确定性规则归并（与正常事件同一 Add 路径）。
	r.wm.Observe(ev)
	r.win.Add(ev)
	// 水位推进后回收超出去重保留范围的旧 ID，状态保持有界。
	r.DedupEvictions += r.dedup.Evict(r.wm.Watermark())
	results, next := r.win.CloseUpTo(r.wm.Watermark(), r.nextSeq)
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
		Generation:   r.cfg.Generation,
		SourceOffset: r.src.Offset(),
		MaxEventTime: r.wm.maxSeen,
		Started:      r.wm.started,
		NextSeq:      r.nextSeq,
		Windows:      toSnapshot(r.win.snapshot()),
		Seen:         r.dedup.snapshot(),
	}
	if err := r.store.Save(cp); err != nil {
		return err
	}
	r.sink.ConfirmUpTo(r.nextSeq - 1)
	return nil
}

// Restore 从最近检查点恢复，按下游实际状态自动选择恢复路径：
//
//   - 下游为空（全新进程 + 全新空下游）：以检查点代际/格式校验为门槛，从源
//     位点 0 全量确定性重放，历史已可见结果逐条重新变得可见，后续结果按确定
//     顺序接上；整体输出与不中断基线逐条一致。
//   - 下游保留着已确认结果（同进程续跑）：回到检查点位点，截断未确认输出，
//     由重放确定性地重新产出；检查点落后于已确认进度则拒绝（位点回退）。
//
// 损坏、格式/代际不兼容、位点回退分别返回可区分错误，绝不静默带病续跑。
func (r *Runtime) Restore() error {
	cp, err := r.store.Load()
	if err != nil {
		return err
	}
	if cp.Generation != r.cfg.Generation {
		return fmt.Errorf("%w: checkpoint generation=%q runtime generation=%q",
			ErrGenerationIncompatible, cp.Generation, r.cfg.Generation)
	}
	if r.sink.ConfirmedSeq() < 0 && r.sink.Pending() == 0 {
		return r.restoreByReplay(cp)
	}
	return r.restoreFromCheckpoint(cp)
}

// restoreByReplay 面向全新空下游：清空内存状态，从位点 0 全量重放。
// 已落盘检查点只用于证明“这是一条合法的同代际延续”；输出完全由可重放输入
// 确定性重建，因此空下游最终看到的历史与后续结果和从头跑一遍一致。
func (r *Runtime) restoreByReplay(cp Checkpoint) error {
	r.reset()
	if err := r.src.SeekOffset(0); err != nil {
		return err
	}
	return nil
}

// restoreFromCheckpoint 面向保留下游：回到检查点位点，未确认结果截断重放。
func (r *Runtime) restoreFromCheckpoint(cp Checkpoint) error {
	// 位点回退：检查点已确认的输出落后于 Sink 中已确认输出，拒绝恢复。
	if cp.NextSeq-1 < r.sink.ConfirmedSeq() {
		return fmt.Errorf("%w: checkpoint nextSeq=%d confirmedSeq=%d",
			ErrOffsetRegression, cp.NextSeq, r.sink.ConfirmedSeq())
	}
	// 源位点越界（回退到不存在的位置）同样拒绝。
	if err := r.src.SeekOffset(cp.SourceOffset); err != nil {
		return err
	}
	r.wm.maxSeen = cp.MaxEventTime
	r.wm.started = cp.Started
	r.nextSeq = cp.NextSeq
	r.win.restore(fromSnapshot(cp.Windows))
	r.dedup.restore(cp.Seen)
	// 未确认输出视为未发生，由重放确定性地重新产出，避免重复投递。
	r.sink.TruncateUnconfirmed()
	return nil
}

// reset 把可变内存状态全部清零，供空下游全量重放使用。
func (r *Runtime) reset() {
	r.wm = NewWatermarkTracker(r.cfg.AllowedLateness)
	r.win = NewWindowManager(r.cfg.WindowSize)
	r.dedup = NewDedupTracker(r.cfg.DedupRetention)
	r.nextSeq = 0
	r.LateDropped = nil
	r.DataErrors = nil
	r.Duplicates = nil
	r.DedupEvictions = 0
}

// NextSeq 返回下一条输出结果的序号（测试用于观察恢复位点）。
func (r *Runtime) NextSeq() int64 { return r.nextSeq }

// Watermark 返回当前水位线。
func (r *Runtime) Watermark() int64 { return r.wm.Watermark() }

// DedupSeen 返回当前保留的已见 ID 数量（有界性测试观察用）。
func (r *Runtime) DedupSeen() int { return r.dedup.Len() }

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
