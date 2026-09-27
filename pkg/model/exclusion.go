package model

import "time"

// ExclusionReason 表示事件未进入窗口计算的确定性原因。
type ExclusionReason int

const (
	// ReasonDataError：数据错误（空键/非法时间/负值）。
	ReasonDataError ExclusionReason = iota + 1
	// ReasonOutOfRangeLate：合法事件，但水位线已超过窗口结束时间 + 允许迟到，
	// 窗口状态已被清除，无法再吸收——这是“时间语义拒绝”，不是数据错误。
	ReasonOutOfRangeLate
	// ReasonDuplicate：重复事件（SeqID 已处理过）。
	ReasonDuplicate
)

func (r ExclusionReason) String() string {
	switch r {
	case ReasonDataError:
		return "DATA_ERROR"
	case ReasonOutOfRangeLate:
		return "OUT_OF_RANGE_LATE"
	case ReasonDuplicate:
		return "DUPLICATE"
	default:
		return "UNKNOWN"
	}
}

// Exclusion 记录一条被排除的事件，确保“超范围迟到”与“数据错误”
// 在下游与审计日志中可明确区分。
type Exclusion struct {
	SeqID      int64
	Key        string
	EventAt    time.Time
	Reason     ExclusionReason
	Detail     string
	ObservedAt time.Time
}
