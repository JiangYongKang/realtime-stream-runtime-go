package engine

// Metrics 是运行时可观测计数（用于测试背压节流与结果可解释性）。
type Metrics struct {
	EventsProcessed    int64
	WatermarksAdvanced int64
	OutputsDelivered   int64
	ExclusionsData     int64
	ExclusionsLate     int64
	Duplicates         int64
	Checkpoints        int64
	// BackpressureBlocks：处理协程因下游容量不足而阻塞的次数。
	BackpressureBlocks int64
	// BackpressureWaitNanos：因背压累计等待时长。
	BackpressureWaitNanos int64
}
