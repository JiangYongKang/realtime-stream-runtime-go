package window

import (
	"errors"
	"slices"
	"time"

	"github.com/highcumontoa/realtime-stream-runtime-go/pkg/checkpoint"
	"github.com/highcumontoa/realtime-stream-runtime-go/pkg/model"
)

// Assigner 将事件时间映射到窗口起点（左闭右开 [start, end)）。
type Assigner interface {
	Assign(t time.Time) (start, end time.Time)
	Size() time.Duration
}

// TumblingAssigner 是固定长度的滚动窗口分配器，窗口边界对齐 UTC 纪元，
// 使任何重放/恢复后的窗口划分都完全确定。
type TumblingAssigner struct {
	size time.Duration
}

// NewTumblingAssigner 创建滚动窗口分配器。
func NewTumblingAssigner(size time.Duration) (*TumblingAssigner, error) {
	if size <= 0 {
		return nil, errors.New("window size must be positive")
	}
	return &TumblingAssigner{size: size}, nil
}

func (a *TumblingAssigner) Size() time.Duration { return a.size }

func (a *TumblingAssigner) Assign(t time.Time) (time.Time, time.Time) {
	t = t.UTC()
	start := t.Truncate(a.size)
	return start, start.Add(a.size)
}

// ApplyOutcome 是处理单条事件后的确定性结论。
type ApplyOutcome int

const (
	// OutcomeAccepted：事件被仍处于开放期（未触发 FIRED）的窗口吸收。
	OutcomeAccepted ApplyOutcome = iota + 1
	// OutcomeAcceptedLate：迟到事件被已 FIRED 但仍在允许迟到保留期内的窗口吸收，
	// 同时确定性地产出一条 CORRECTION。
	OutcomeAcceptedLate
	// OutcomeDuplicate：SeqID 重复。
	OutcomeDuplicate
	// OutcomeOutOfRangeLate：事件合法，但其窗口已结束且超出允许迟到范围（窗口已清除）。
	OutcomeOutOfRangeLate
	// OutcomeDataError：数据本身不合法。
	OutcomeDataError
)

// winState 是单个窗口的状态。
type winState struct {
	end   time.Time
	fired bool
	keys  map[string]*keyAgg
}

// keyAgg 是单窗口单 key 的聚合；Version 即输出版本（FIRED=1，每次迟到归并 +1）。
type keyAgg struct {
	sum     float64
	count   int64
	version int
}

// Table 是事件时间窗口聚合表，持有水位线、窗口状态与去重集合。
// 单协程使用：所有事件处理与水位线推进必须在同一 goroutine 中串行进行。
type Table struct {
	assigner        Assigner
	allowedLateness time.Duration

	watermark time.Time
	windows   map[time.Time]*winState
	seen      map[int64]struct{}
}

// NewTable 创建窗口聚合表。
func NewTable(assigner Assigner, allowedLateness time.Duration) *Table {
	return &Table{
		assigner:        assigner,
		allowedLateness: allowedLateness,
		windows:         make(map[time.Time]*winState),
		seen:            make(map[int64]struct{}),
	}
}

// ApplyEvent 将一条事件确定性地纳入计算或给出排除结论。
// 返回的 *WindowOutput 仅在结论为 OutcomeAcceptedLate 时非空（迟到归并的 CORRECTION）。
func (t *Table) ApplyEvent(e model.Event) (ApplyOutcome, *model.WindowOutput) {
	if err := e.Validate(); err != nil {
		return OutcomeDataError, nil
	}
	if _, dup := t.seen[e.SeqID]; dup {
		return OutcomeDuplicate, nil
	}

	start, end := t.assigner.Assign(e.EventAt)
	w, exists := t.windows[start]
	if !exists {
		// 水位线推进后，超过 (窗口结束 + 允许迟到) 的窗口已被清除；
		// 此时迟到的事件不得再被吸收——这是确定性的“超范围迟到”。
		// 注意：能进入这里说明事件本身合法，绝不与数据错误混淆。
		if !end.After(t.watermark.Add(-t.allowedLateness)) {
			return OutcomeOutOfRangeLate, nil
		}
		w = &winState{end: end, keys: make(map[string]*keyAgg)}
		t.windows[start] = w
	}

	agg := w.keys[e.Key]
	if agg == nil {
		agg = &keyAgg{}
		w.keys[e.Key] = agg
	}
	agg.sum += e.Value
	agg.count++
	t.seen[e.SeqID] = struct{}{}

	if w.fired {
		// 窗口已关闭（FIRED 已发出），但仍处于允许迟到保留期：
		// 按确定性规则归并——累加同一 key 的值，并以版本号 +1 的 CORRECTION 更正。
		agg.version++
		return OutcomeAcceptedLate, &model.WindowOutput{
			WindowStart: start,
			WindowEnd:   end,
			Key:         e.Key,
			Sum:         agg.sum,
			Count:       agg.count,
			Kind:        model.KindCorrection,
			Version:     agg.version,
		}
	}
	return OutcomeAccepted, nil
}

// AdvanceWatermark 将水位线推进到 newWM（不允许回退；较小的推进是空操作）。
// 同一次推进中先触发到期窗口的 FIRED，再清除超出允许迟到范围的窗口，顺序固定：
//  1. fired：窗口 end <= 新水位线 且尚未 FIRED —— 每个 key 产出版本 1 的 FIRED；
//  2. purged：窗口 end + allowedLateness <= 新水位线 —— 状态被删除，
//     此后属于这些窗口的事件一律按 OutcomeOutOfRangeLate 处理。
func (t *Table) AdvanceWatermark(newWM time.Time) (fired []model.WindowOutput, purgedStarts []time.Time) {
	if !newWM.After(t.watermark) {
		return nil, nil
	}

	starts := make([]time.Time, 0, len(t.windows))
	for st := range t.windows {
		starts = append(starts, st)
	}
	slices.SortFunc(starts, func(a, b time.Time) int { return a.Compare(b) })

	for _, st := range starts {
		w := t.windows[st]
		if w == nil {
			continue
		}
		if !w.fired && !w.end.After(newWM) {
			keys := make([]string, 0, len(w.keys))
			for k := range w.keys {
				keys = append(keys, k)
			}
			slices.Sort(keys)
			for _, k := range keys {
				agg := w.keys[k]
				agg.version = 1 // 首次（也是唯一一次）FIRED 固定为版本 1
				fired = append(fired, model.WindowOutput{
					WindowStart: st,
					WindowEnd:   w.end,
					Key:         k,
					Sum:         agg.sum,
					Count:       agg.count,
					Kind:        model.KindFired,
					Version:     1,
				})
			}
			w.fired = true
		}
	}

	for _, st := range starts {
		w := t.windows[st]
		if w == nil {
			continue
		}
		if !w.end.Add(t.allowedLateness).After(newWM) {
			delete(t.windows, st)
			purgedStarts = append(purgedStarts, st)
		}
	}

	t.watermark = newWM
	return fired, purgedStarts
}

// Watermark 返回当前水位线。
func (t *Table) Watermark() time.Time { return t.watermark }

// Snapshot 将表状态导出为可持久化的检查点状态。
func (t *Table) Snapshot(position int64) *checkpoint.State {
	st := &checkpoint.State{
		Version:    checkpoint.Version,
		Position:   position,
		Watermark:  t.watermark,
		WindowMeta: make(map[time.Time]checkpoint.WindowMeta, len(t.windows)),
		Windows:    make(map[checkpoint.WinKey]checkpoint.AggState),
		Seen:       make([]int64, 0, len(t.seen)),
	}
	for seq := range t.seen {
		st.Seen = append(st.Seen, seq)
	}
	slices.Sort(st.Seen)
	// map 按窗口起点有序遍历，保证字节级确定性。
	ws := make([]time.Time, 0, len(t.windows))
	for wStart := range t.windows {
		ws = append(ws, wStart)
	}
	slices.SortFunc(ws, func(a, b time.Time) int { return a.Compare(b) })
	for _, wStart := range ws {
		w := t.windows[wStart]
		st.WindowMeta[wStart] = checkpoint.WindowMeta{End: w.end, Fired: w.fired}
		ks := make([]string, 0, len(w.keys))
		for k := range w.keys {
			ks = append(ks, k)
		}
		slices.Sort(ks)
		for _, k := range ks {
			agg := w.keys[k]
			st.Windows[checkpoint.WinKey{WindowStart: wStart, Key: k}] = checkpoint.AggState{
				Sum: agg.sum, Count: agg.count, Version: agg.version,
			}
		}
	}
	return st
}

// Restore 从检查点状态恢复表（水位线、窗口、去重集合）。
func (t *Table) Restore(state *checkpoint.State) error {
	if state == nil {
		return errors.New("cannot restore nil state")
	}
	t.watermark = state.Watermark.UTC()
	t.windows = make(map[time.Time]*winState)
	t.seen = make(map[int64]struct{}, len(state.Seen))
	for _, seq := range state.Seen {
		t.seen[seq] = struct{}{}
	}
	for wk, agg := range state.Windows {
		st := wk.WindowStart.UTC()
		w := t.windows[st]
		if w == nil {
			meta := state.WindowMeta[st]
			w = &winState{end: meta.End.UTC(), fired: meta.Fired, keys: make(map[string]*keyAgg)}
			t.windows[st] = w
		}
		w.keys[wk.Key] = &keyAgg{sum: agg.Sum, count: agg.Count, version: agg.Version}
	}
	return nil
}
