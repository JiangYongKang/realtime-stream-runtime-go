package stream

import (
	"container/heap"
	"sort"
)

// SeenEvent 是已见事件 ID 的可序列化形式（供检查点保存去重状态）。
type SeenEvent struct {
	ID        string `json:"id"`
	Key       string `json:"key"`
	EventTime int64  `json:"eventTime"`
	Value     int64  `json:"value"`
}

// DupRecord 记录一次被识别出的重复投递（与数据错误、超范围迟到分开统计）。
type DupRecord struct {
	Event     Event
	Watermark int64
	Reason    string
}

// seenEntry 已见事件的内存形式。
type seenEntry struct {
	id        string
	key       string
	eventTime int64
	value     int64
}

// DedupTracker 基于事件 ID 的幂等去重跟踪器，状态按事件时间保留范围有界回收。
// 事件的"身份"为 ID；同 ID 且内容（key/eventTime/value）完全一致视为重复投递，
// 同 ID 但内容不一致视为数据错误（冲突），二者区分统计。
type DedupTracker struct {
	retention int64
	seen      map[string]*seenEntry
	order     seenHeap // 按事件时间排序的最小堆，驱动回收
}

// NewDedupTracker 构造去重跟踪器。retention 为事件时间维度的保留范围（毫秒）：
// 当 watermark - eventTime >= retention 时回收该已见 ID；retention=0 表示永不回收。
func NewDedupTracker(retention int64) *DedupTracker {
	return &DedupTracker{retention: retention, seen: map[string]*seenEntry{}}
}

const (
	dedupNew       = 0 // 新事件
	dedupDuplicate = 1 // 重复投递：ID 与内容均一致
	dedupConflict  = 2 // ID 相同但内容冲突（数据错误）
)

// Classify 只读判定事件为新事件/重复/冲突，不改变任何状态。
func (d *DedupTracker) Classify(ev Event) int {
	e, ok := d.seen[ev.ID]
	if !ok {
		return dedupNew
	}
	if e.key == ev.Key && e.eventTime == ev.EventTime && e.value == ev.Value {
		return dedupDuplicate
	}
	return dedupConflict
}

// Commit 登记一条新事件的 ID（仅对 Classify 返回 dedupNew 且确定被吸收的事件调用）。
func (d *DedupTracker) Commit(ev Event) {
	e := &seenEntry{id: ev.ID, key: ev.Key, eventTime: ev.EventTime, value: ev.Value}
	d.seen[ev.ID] = e
	heap.Push(&d.order, e)
}

// Evict 按当前水位线回收超出保留范围的已见 ID，返回回收条数。
// 落在保留范围内的 ID 一定保留，去重能力不受回收影响。
func (d *DedupTracker) Evict(watermark int64) int {
	if d.retention <= 0 {
		return 0
	}
	n := 0
	for d.order.Len() > 0 {
		oldest := d.order[0]
		if watermark-oldest.eventTime < d.retention {
			break // 堆按事件时间有序，最老的仍在保留范围内，后续更新
		}
		heap.Pop(&d.order)
		delete(d.seen, oldest.id)
		n++
	}
	return n
}

// Len 返回当前保留的已见 ID 数量（供有界性测试观察）。
func (d *DedupTracker) Len() int { return len(d.seen) }

// snapshot / restore 供检查点使用。快照按 ID 排序，落盘内容确定。
func (d *DedupTracker) snapshot() []SeenEvent {
	out := make([]SeenEvent, 0, len(d.seen))
	for _, e := range d.seen {
		out = append(out, SeenEvent{ID: e.id, Key: e.key, EventTime: e.eventTime, Value: e.value})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func (d *DedupTracker) restore(seen []SeenEvent) {
	d.seen = map[string]*seenEntry{}
	d.order = nil
	for _, s := range seen {
		e := &seenEntry{id: s.ID, key: s.Key, eventTime: s.EventTime, value: s.Value}
		d.seen[s.ID] = e
		heap.Push(&d.order, e)
	}
}

// seenHeap 按事件时间升序的最小堆；事件时间相同按 ID 排序，保证回收顺序确定。
type seenHeap []*seenEntry

func (h seenHeap) Len() int { return len(h) }
func (h seenHeap) Less(i, j int) bool {
	if h[i].eventTime != h[j].eventTime {
		return h[i].eventTime < h[j].eventTime
	}
	return h[i].id < h[j].id
}
func (h seenHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *seenHeap) Push(x any)   { *h = append(*h, x.(*seenEntry)) }
func (h *seenHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}
