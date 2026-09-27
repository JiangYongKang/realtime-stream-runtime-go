package model

import (
	"errors"
	"strconv"
	"time"
)

// Event 是一条输入事件（以事件时间为时间语义）。
type Event struct {
	SeqID     int64     // 源端唯一序号（用作去重身份与位点）
	Key       string    // 分组键
	Value     float64   // 聚合值
	EventAt   time.Time // 事件时间
	ArrivedAt time.Time // 到达运行时的处理时间（仅用于诊断）
}

// StreamItem 是重放流中的一个元素：要么是事件，要么是水位线推进。
type StreamItem struct {
	Event     *Event
	Watermark time.Time // 零值表示非水位线项
}

// IsWatermark 报告该项是否为水位线标记。
func (s StreamItem) IsWatermark() bool {
	return s.Event == nil
}

// ErrDataError 表示数据本身的错误（空键/非法时间/负值），
// 与“合法但超范围迟到”的事件严格区分。
var ErrDataError = errors.New("data error")

// DataError 携带一条数据错误的明细。
type DataError struct {
	SeqID  int64
	Reason string
}

func (d *DataError) Error() string {
	if d == nil {
		return ""
	}
	return ErrDataError.Error() + ": seq=" + strconv.FormatInt(d.SeqID, 10) + " reason=" + d.Reason
}

// Validate 校验事件数据本身是否合法。
// 数据错误（ReasonDataError）与超范围迟到（ReasonOutOfRangeLate）是两类完全不同的问题：
// 前者是字段不合法，后者是时间语义上无法再纳入计算。
func (e Event) Validate() error {
	if e.SeqID < 0 {
		return &DataError{SeqID: e.SeqID, Reason: "seq id must be non-negative"}
	}
	if e.Key == "" {
		return &DataError{SeqID: e.SeqID, Reason: "key must not be empty"}
	}
	if e.Value < 0 {
		return &DataError{SeqID: e.SeqID, Reason: "value must not be negative"}
	}
	if e.EventAt.IsZero() {
		return &DataError{SeqID: e.SeqID, Reason: "event time must not be zero"}
	}
	return nil
}
