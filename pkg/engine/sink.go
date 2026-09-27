package engine

import (
	"errors"
	"sync"

	"github.com/highcumontoa/realtime-stream-runtime-go/pkg/model"
)

// ErrSinkClosed 表示向已关闭的下游投递。
var ErrSinkClosed = errors.New("sink closed")

// Sink 是有界下游：
//   - 容量即最大在途结果数。处理协程在容量耗尽时于 Deliver 处阻塞，
//     形成对输入读取的节流，而不是在内存中无限缓冲；
//   - 单个 worker 严格按投递顺序调用处理器并确认，恢复后也不会乱序；
//   - Confirmed 视图只包含已被处理器确认的结果，供检查点记录“已确认”边界。
type Sink struct {
	tokens chan struct{}
	jobs   chan sinkJob
	done   chan struct{}

	mu        sync.Mutex
	outputs   []model.WindowOutput
	exclusion []model.Exclusion
	closed    bool

	handleOut func(model.WindowOutput) error
	handleEx  func(model.Exclusion) error
}

type sinkJob struct {
	out *model.WindowOutput
	ex  *model.Exclusion
}

// NewSink 创建容量为 capacity 的有界下游（容量即最大在途结果数）。
func NewSink(capacity int) *Sink {
	if capacity <= 0 {
		capacity = 1
	}
	s := &Sink{
		tokens:    make(chan struct{}, capacity),
		jobs:      make(chan sinkJob),
		done:      make(chan struct{}),
		handleOut: func(model.WindowOutput) error { return nil },
		handleEx:  func(model.Exclusion) error { return nil },
	}
	go s.worker()
	return s
}

// worker 是唯一调用处理器、唯一追加 Confirmed 的 goroutine，保证 FIFO 与可见性一致。
func (s *Sink) worker() {
	defer close(s.done)
	for j := range s.jobs {
		if j.out != nil {
			if err := s.handleOut(*j.out); err != nil {
				// 处理器失败：该结果未确认，释放令牌后直接停止，
				// 绝不静默丢弃或跳过；由上层决定重启重放。
				<-s.tokens
				return
			}
			s.mu.Lock()
			s.outputs = append(s.outputs, *j.out)
			s.mu.Unlock()
		} else {
			if err := s.handleEx(*j.ex); err != nil {
				<-s.tokens
				return
			}
			s.mu.Lock()
			s.exclusion = append(s.exclusion, *j.ex)
			s.mu.Unlock()
		}
		<-s.tokens
	}
}

// Deliver 投递一条窗口结果；下游处理能力不足（在途数达容量）时阻塞以形成背压。
func (s *Sink) Deliver(o model.WindowOutput) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrSinkClosed
	}
	s.mu.Unlock()
	s.tokens <- struct{}{} // 获取在途配额：满了就在此阻塞 → 输入读取被节制
	s.jobs <- sinkJob{out: &o}
	return nil
}

// DeliverExclusion 投递一条已确认的排除记录（同样有界、保序）。
func (s *Sink) DeliverExclusion(e model.Exclusion) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrSinkClosed
	}
	s.mu.Unlock()
	s.tokens <- struct{}{}
	s.jobs <- sinkJob{ex: &e}
	return nil
}

// InFlight 返回当前在途（已 Deliver 但未被 worker 确认）的结果数。
func (s *Sink) InFlight() int { return len(s.tokens) }

// ConfirmedView 是已确认数据的只读视图。
type ConfirmedView struct {
	Outputs    []model.WindowOutput
	Exclusions []model.Exclusion
}

// Confirmed 返回已确认结果（窗口输出 + 排除）的快照，用于检查点与测试。
func (s *Sink) Confirmed() ConfirmedView {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]model.WindowOutput, len(s.outputs))
	copy(out, s.outputs)
	exs := make([]model.Exclusion, len(s.exclusion))
	copy(exs, s.exclusion)
	return ConfirmedView{Outputs: out, Exclusions: exs}
}

// Drain 等待所有在途结果被确认（检查点前调用，确保检查点记录的都是“已可见”结果）。
func (s *Sink) Drain() {
	// 占满全部令牌并归还：若还有在途结果，获取会阻塞直到 worker 释放。
	for i := 0; i < cap(s.tokens); i++ {
		s.tokens <- struct{}{}
	}
	for i := 0; i < cap(s.tokens); i++ {
		<-s.tokens
	}
}

// Truncate 恢复时把已确认视图替换为检查点记录的内容。
// 由于崩溃只发生在检查点边界，检查点之后的结果在“已确认视图”中本来就不存在，
// 这里同时重置在途配额，保证恢复后不重不丢、不重复投递。
func (s *Sink) Truncate(outputs []model.WindowOutput, exclusions []model.Exclusion) {
	s.mu.Lock()
	s.outputs = make([]model.WindowOutput, len(outputs))
	copy(s.outputs, outputs)
	s.exclusion = make([]model.Exclusion, len(exclusions))
	copy(s.exclusion, exclusions)
	s.mu.Unlock()
	// 排空可能遗留的在途令牌（新 Sink 场景下本就为空）。
drain:
	for {
		select {
		case <-s.tokens:
		default:
			break drain
		}
	}
}

// SetHandler 安装模拟下游处理器；handler 返回前结果不算确认。
func (s *Sink) SetHandler(handler func(model.WindowOutput) error) {
	if handler == nil {
		return
	}
	s.handleOut = handler
}

// SetExclusionHandler 安装排除记录处理器。
func (s *Sink) SetExclusionHandler(handler func(model.Exclusion) error) {
	if handler == nil {
		return
	}
	s.handleEx = handler
}

// Close 等待在途结果排空后关闭 worker。
func (s *Sink) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.mu.Unlock()
	close(s.jobs)
	<-s.done
	return nil
}
