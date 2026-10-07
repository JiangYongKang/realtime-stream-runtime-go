package stream

import "sort"

// DuplicateRecord 记录一条被识别为重复投递的事件。
// 与数据错误（DataErrors）、超范围迟到（LateDropped）分别统计，便于排查。
type DuplicateRecord struct {
	Event Event
}

// Deduper 在配置的事件时间保留范围内识别重复事件，状态随窗口关闭做回收。
// 身份集合只保留事件时间 >= maxEventTime-retention 的条目，容量随保留范围有界，
// 不随运行时间无限增长。
type Deduper struct {
	retention int64
	entries   map[string]int64 // 事件 ID -> 事件时间
}

// NewDeduper 构造去重器，retention 为事件时间保留范围（毫秒）。
func NewDeduper(retention int64) *Deduper {
	return &Deduper{retention: retention, entries: map[string]int64{}}
}

// Seen 判断事件 ID 是否已经在保留范围内出现过。
func (d *Deduper) Seen(ev Event) bool {
	_, ok := d.entries[ev.ID]
	return ok
}

// Mark 记录一个已处理的事件 ID。
func (d *Deduper) Mark(ev Event) {
	d.entries[ev.ID] = ev.EventTime
}

// Prune 回收事件时间严格早于 horizon 的身份条目；
// horizon 之内的重复仍可识别，horizon 之外的窗口早已关闭、结果不可能再被改变。
func (d *Deduper) Prune(horizon int64) {
	for id, et := range d.entries {
		if et < horizon {
			delete(d.entries, id)
		}
	}
}

// Count 返回当前保留的身份条目数（测试用于验证状态有界）。
func (d *Deduper) Count() int { return len(d.entries) }

// dedupEntry 是去重状态的可持久化条目。
type dedupEntry struct {
	ID        string `json:"id"`
	EventTime int64  `json:"eventTime"`
}

// snapshot / restore 供检查点使用。
func (d *Deduper) snapshot() []dedupEntry {
	if len(d.entries) == 0 {
		return nil
	}
	out := make([]dedupEntry, 0, len(d.entries))
	for id, et := range d.entries {
		out = append(out, dedupEntry{ID: id, EventTime: et})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].EventTime != out[j].EventTime {
			return out[i].EventTime < out[j].EventTime
		}
		return out[i].ID < out[j].ID
	})
	return out
}

func (d *Deduper) restore(entries []dedupEntry) {
	d.entries = make(map[string]int64, len(entries))
	for _, e := range entries {
		d.entries[e.ID] = e.EventTime
	}
}
