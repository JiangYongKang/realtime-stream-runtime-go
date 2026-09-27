package stream

import (
	"context"
	"sync"
)

// Sink 是有界缓冲的下游出口：缓冲满时写入阻塞，从而对上游形成背压。
// 已确认（confirmed）的结果对外可见；未确认结果在重启语义下视为未发生。
type Sink struct {
	mu           sync.Mutex
	cond         *sync.Cond
	buf          []Result // 未确认缓冲区（有界）
	cap          int
	confirmed    []Result // 已确认、对外可见
	confirmedSeq int64    // 已确认到的最大 Seq（含），-1 表示无
}

// NewSink 构造容量为 cap 的有界 Sink。
func NewSink(cap int) *Sink {
	s := &Sink{cap: cap, confirmedSeq: -1}
	s.cond = sync.NewCond(&s.mu)
	return s
}

// Write 写入一条结果；缓冲满时阻塞直至有确认释放空间或 ctx 取消。
func (s *Sink) Write(ctx context.Context, r Result) error {
	stop := context.AfterFunc(ctx, func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.cond.Broadcast()
	})
	defer stop()
	s.mu.Lock()
	defer s.mu.Unlock()
	for len(s.buf) >= s.cap {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		s.cond.Wait()
	}
	s.buf = append(s.buf, r)
	return nil
}

// ConfirmUpTo 将 Seq <= seq 的缓冲结果转为已确认（对外可见）。
func (s *Sink) ConfirmUpTo(seq int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var rest []Result
	for _, r := range s.buf {
		if r.Seq <= seq {
			s.confirmed = append(s.confirmed, r)
			if r.Seq > s.confirmedSeq {
				s.confirmedSeq = r.Seq
			}
		} else {
			rest = append(rest, r)
		}
	}
	s.buf = rest
	s.cond.Broadcast()
}

// Pending 返回当前未确认缓冲中的条数。
func (s *Sink) Pending() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.buf)
}

// Confirmed 返回所有已确认（可见）结果，按 Seq 有序。
func (s *Sink) Confirmed() []Result {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Result, len(s.confirmed))
	copy(out, s.confirmed)
	return out
}

// ConfirmedSeq 返回已确认的最大 Seq。
func (s *Sink) ConfirmedSeq() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.confirmedSeq
}

// TruncateUnconfirmed 丢弃所有未确认的缓冲结果。
// 重启恢复时调用：未确认输出视为未发生，由恢复后的重放确定性地产出。
func (s *Sink) TruncateUnconfirmed() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.buf = nil
	s.cond.Broadcast()
}
