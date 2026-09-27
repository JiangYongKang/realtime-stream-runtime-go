package stream

// WatermarkTracker 按“最大事件时间 - 允许迟到”推进水位线。
type WatermarkTracker struct {
	allowedLateness int64
	maxSeen         int64
	started         bool
}

// NewWatermarkTracker 构造水位线跟踪器，allowedLateness 为允许的迟到范围（毫秒）。
func NewWatermarkTracker(allowedLateness int64) *WatermarkTracker {
	return &WatermarkTracker{allowedLateness: allowedLateness}
}

// Observe 观察一条事件并推进水位线。
func (w *WatermarkTracker) Observe(ev Event) {
	if !w.started || ev.EventTime > w.maxSeen {
		w.maxSeen = ev.EventTime
		w.started = true
	}
}

// Watermark 返回当前水位线：maxSeen - allowedLateness。
func (w *WatermarkTracker) Watermark() int64 {
	if !w.started {
		return 0
	}
	return w.maxSeen - w.allowedLateness
}

// IsLate 判断事件是否为迟到事件（事件时间不超过当前水位线）。
func (w *WatermarkTracker) IsLate(ev Event) bool {
	return w.started && ev.EventTime <= w.Watermark()
}
