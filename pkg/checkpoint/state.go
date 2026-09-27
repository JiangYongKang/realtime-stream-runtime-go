package checkpoint

import (
	"errors"
	"time"
)

// Version 是检查点格式版本，版本不兼容时恢复必须被拒绝。
const Version = 1

// WinKey 唯一标识一个窗口内的一个 key 聚合。
type WinKey struct {
	WindowStart time.Time
	Key         string
}

// AggState 是单个 key 的聚合状态。
type AggState struct {
	Sum     float64
	Count   int64
	Version int
}

// WindowMeta 是窗口级元数据：结束时间与是否已触发 FIRED。
type WindowMeta struct {
	End   time.Time
	Fired bool
}

// OutputRecord 是已确认投递的下游结果，用于恢复后截断重放、保证不重不丢。
type OutputRecord struct {
	WindowStart time.Time
	WindowEnd   time.Time
	Key         string
	Sum         float64
	Count       int64
	Kind        int
	Version     int
}

// ExclusionRecord 是已确认的排除记录。
type ExclusionRecord struct {
	SeqID      int64
	Key        string
	EventAt    time.Time
	Reason     int
	Detail     string
	ObservedAt time.Time
}

// State 是检查点携带的完整运行时状态。
type State struct {
	Version    int
	Position   int64 // 源位点：最后一条已处理元素的 SeqID（水位线项沿用前一事件位点）
	Watermark  time.Time
	WindowMeta map[time.Time]WindowMeta
	Windows    map[WinKey]AggState
	Seen       []int64 // 已处理事件的 SeqID 集合（去重），序列化前排序以保持确定性
	Outputs    []OutputRecord
	Exclusions []ExclusionRecord
}

var errNotImplemented = errors.New("not implemented")
