package engine

import (
	"time"

	"github.com/highcumontoa/realtime-stream-runtime-go/pkg/model"
)

// ReplaySource 是可重放的输入源：从中断位点恢复后，
// 必须重新从开头提供流；SeekAfter 用于跳过已确认事件，只重放未确认部分。
type ReplaySource interface {
	// Next 返回下一个流元素；ok=false 表示流结束。
	Next() (model.StreamItem, bool)
	// SeekAfter 使后续 Next 跳过 SeqID <= afterSeq 的事件（水位线项始终保留，
	// 恢复后旧水位线推进在窗口表上是空操作，新水位线照常推进，结果确定）。
	SeekAfter(afterSeq int64)
	// Reset 回到流的开头（用于恢复重放）。
	Reset()
}

// StaticSource 是内存中的确定性可重放源，用固定的乱序/重复事件序列模拟真实接入。
type StaticSource struct {
	items []model.StreamItem
	pos   int
}

// NewStaticSource 用固定流元素构造可重放源。
func NewStaticSource(items []model.StreamItem) *StaticSource {
	cp := make([]model.StreamItem, len(items))
	copy(cp, items)
	return &StaticSource{items: cp}
}

// EventItem 构造一个事件流元素（便捷函数）。
func EventItem(seq int64, key string, value float64, eventAt time.Time) model.StreamItem {
	return model.StreamItem{Event: &model.Event{
		SeqID: seq, Key: key, Value: value, EventAt: eventAt, ArrivedAt: time.Now().UTC(),
	}}
}

// WatermarkItem 构造一个水位线流元素。
func WatermarkItem(wm time.Time) model.StreamItem {
	return model.StreamItem{Watermark: wm.UTC()}
}

func (s *StaticSource) Next() (model.StreamItem, bool) {
	for s.pos < len(s.items) {
		it := s.items[s.pos]
		s.pos++
		return it, true
	}
	return model.StreamItem{}, false
}

// SeekAfter 跳过已确认事件；水位线项在恢复重放中全部保留（旧推进幂等）。
func (s *StaticSource) SeekAfter(afterSeq int64) {
	filtered := s.items[:0]
	for _, it := range s.items {
		if !it.IsWatermark() && it.Event.SeqID <= afterSeq {
			continue
		}
		filtered = append(filtered, it)
	}
	s.items = filtered
	s.pos = 0
}

// Reset 回到流开头。
func (s *StaticSource) Reset() { s.pos = 0 }
