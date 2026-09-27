package stream

import (
	"context"
	"errors"
	"fmt"
)

// ErrDataCorrupt 表示输入事件本身损坏（与超范围迟到区分）。
var ErrDataCorrupt = errors.New("data corrupt")

// Runtime 编排：可重放源 -> 水位线/窗口 -> 有界 Sink，并负责检查点与恢复。
type Runtime struct {
	src   Source
	wm    *WatermarkTracker
	win   *WindowManager
	sink  *Sink
	store *Store

	nextSeq int64

	// LateDropped 记录所有被丢弃的超范围迟到事件。
	LateDropped []LateRecord
	// DataErrors 记录所有数据错误（与迟到丢弃区分）。
	DataErrors []error
}

// New 构造运行时。windowSize 为窗口宽度，allowedLateness 为允许迟到范围（毫秒）。
func New(src Source, windowSize, allowedLateness int64, sink *Sink, store *Store) *Runtime {
	return &Runtime{
		src:   src,
		wm:    NewWatermarkTracker(allowedLateness),
		win:   NewWindowManager(windowSize),
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
	r.win.Add(ev)
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
		SourceOffset: r.src.Offset(),
		MaxEventTime: r.wm.maxSeen,
		Started:      r.wm.started,
		NextSeq:      r.nextSeq,
		Windows:      toSnapshot(r.win.snapshot()),
	}
	if err := r.store.Save(cp); err != nil {
		return err
	}
	r.sink.ConfirmUpTo(r.nextSeq - 1)
	return nil
}

// Restore 从最近检查点恢复。损坏、版本不兼容、位点回退分别返回可区分错误。
func (r *Runtime) Restore() error {
	cp, err := r.store.Load()
	if err != nil {
		return err
	}
	// 位点回退：检查点已确认的输出落后于 Sink 中已确认输出，拒绝恢复。
	if cp.NextSeq-1 < r.sink.ConfirmedSeq() {
		return ErrOffsetRegression
	}
	// 源位点越界（回退到不存在的位置）同样拒绝。
	if err := r.src.SeekOffset(cp.SourceOffset); err != nil {
		return err
	}
	r.wm.maxSeen = cp.MaxEventTime
	r.wm.started = cp.Started
	r.nextSeq = cp.NextSeq
	r.win.restore(fromSnapshot(cp.Windows))
	// 未确认输出视为未发生，由重放确定性地重新产出，避免重复投递。
	r.sink.TruncateUnconfirmed()
	return nil
}

// NextSeq 返回下一条输出结果的序号（测试用于观察恢复位点）。
func (r *Runtime) NextSeq() int64 { return r.nextSeq }

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
