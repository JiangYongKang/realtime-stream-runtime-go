package stream

// Event 是输入流中的一条记录，携带事件时间。
type Event struct {
	Key       string
	EventTime int64 // 事件时间，毫秒
	Value     int64
}

// Source 是可重放的输入事件流。Offset 标识下一条待读事件的位点。
type Source interface {
	// Next 返回下一条事件；ok=false 表示流已结束。
	Next() (ev Event, ok bool, err error)
	// Offset 返回下一条待读事件的位点。
	Offset() int64
	// SeekOffset 将读取位点回退/推进到 offset（用于恢复重放）。
	SeekOffset(offset int64) error
}

// SliceSource 基于内存切片实现的可重放 Source。
type SliceSource struct {
	events []Event
	pos    int
}

// NewSliceSource 构造一个可重放的事件源。
func NewSliceSource(events []Event) *SliceSource {
	return &SliceSource{events: events}
}

func (s *SliceSource) Next() (Event, bool, error) {
	if s.pos >= len(s.events) {
		return Event{}, false, nil
	}
	ev := s.events[s.pos]
	s.pos++
	return ev, true, nil
}

func (s *SliceSource) Offset() int64 { return int64(s.pos) }

func (s *SliceSource) SeekOffset(offset int64) error {
	if offset < 0 || offset > int64(len(s.events)) {
		return ErrOffsetRegression
	}
	s.pos = int(offset)
	return nil
}
