package stream

// Result 是一个窗口关闭后产出的聚合结果。
type Result struct {
	WindowStart int64
	WindowEnd   int64
	Key         string
	Count       int64
	Sum         int64
	Seq         int64 // 全局单调递增的输出序号，保证输出有序
}

// LateRecord 记录一条被丢弃的超范围迟到事件（与数据错误区分）。
type LateRecord struct {
	Event       Event
	WindowStart int64
	WindowEnd   int64
	Watermark   int64
	Reason      string
}

// windowState 单个窗口的聚合状态。
type windowState struct {
	count int64
	sum   int64
}

// WindowManager 管理事件时间滚动窗口与迟到事件归并。
type WindowManager struct {
	size    int64
	windows map[int64]map[string]*windowState // windowStart -> key -> state
}

// NewWindowManager 构造窗口管理器，size 为窗口宽度（毫秒）。
func NewWindowManager(size int64) *WindowManager {
	return &WindowManager{size: size, windows: map[int64]map[string]*windowState{}}
}

// WindowStart 返回事件所属窗口的起始时间。
func (m *WindowManager) WindowStart(eventTime int64) int64 {
	return eventTime - mod(eventTime, m.size)
}

func mod(v, m int64) int64 {
	r := v % m
	if r < 0 {
		r += m
	}
	return r
}

// Add 将事件归并进其所属窗口（确定性规则：计数 +1、求和累加）。
func (m *WindowManager) Add(ev Event) {
	start := m.WindowStart(ev.EventTime)
	keys, ok := m.windows[start]
	if !ok {
		keys = map[string]*windowState{}
		m.windows[start] = keys
	}
	st, ok := keys[ev.Key]
	if !ok {
		st = &windowState{}
		keys[ev.Key] = st
	}
	st.count++
	st.sum += ev.Value
}

// CloseUpTo 关闭所有 end <= watermark 的窗口，按 (windowStart, key) 确定性顺序产出结果。
func (m *WindowManager) CloseUpTo(watermark int64, nextSeq int64) ([]Result, int64) {
	var starts []int64
	for start := range m.windows {
		if start+m.size <= watermark {
			starts = append(starts, start)
		}
	}
	sortInt64s(starts)
	var out []Result
	for _, start := range starts {
		keys := m.windows[start]
		for _, k := range sortedKeys(keys) {
			st := keys[k]
			out = append(out, Result{
				WindowStart: start,
				WindowEnd:   start + m.size,
				Key:         k,
				Count:       st.count,
				Sum:         st.sum,
				Seq:         nextSeq,
			})
			nextSeq++
		}
		delete(m.windows, start)
	}
	return out, nextSeq
}

// IsClosed 判断事件所属窗口是否已关闭（不再吸收迟到事件）。
func (m *WindowManager) IsClosed(ev Event, watermark int64) bool {
	return m.WindowStart(ev.EventTime)+m.size <= watermark
}

// snapshot / restore 供检查点使用。
func (m *WindowManager) snapshot() map[int64]map[string]windowState {
	out := map[int64]map[string]windowState{}
	for start, keys := range m.windows {
		inner := map[string]windowState{}
		for k, st := range keys {
			inner[k] = *st
		}
		out[start] = inner
	}
	return out
}

func (m *WindowManager) restore(snap map[int64]map[string]windowState) {
	m.windows = map[int64]map[string]*windowState{}
	for start, keys := range snap {
		inner := map[string]*windowState{}
		for k, st := range keys {
			s := st
			inner[k] = &s
		}
		m.windows[start] = inner
	}
}

func sortInt64s(v []int64) {
	for i := 1; i < len(v); i++ {
		for j := i; j > 0 && v[j] < v[j-1]; j-- {
			v[j], v[j-1] = v[j-1], v[j]
		}
	}
}

func sortedKeys(m map[string]*windowState) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	for i := 1; i < len(keys); i++ {
		for j := i; j > 0 && keys[j] < keys[j-1]; j-- {
			keys[j], keys[j-1] = keys[j-1], keys[j]
		}
	}
	return keys
}
