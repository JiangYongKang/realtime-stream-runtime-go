package stream

import (
	"context"
	"strconv"
	"testing"
	"time"
)

func backpressureEvents() []Event {
	var evs []Event
	// 4 个窗口，每窗口 2 个 key，共 8 条输出。
	for w := int64(0); w < 4; w++ {
		evs = append(evs,
			Event{ID: "bp-a-" + strconv.FormatInt(w, 10), Key: "a", EventTime: w*10 + 1, Value: w + 1},
			Event{ID: "bp-b-" + strconv.FormatInt(w, 10), Key: "b", EventTime: w*10 + 2, Value: w + 2},
		)
	}
	// 推进水位线关闭所有窗口。
	evs = append(evs, Event{ID: "bp-z", Key: "z", EventTime: 100, Value: 0})
	return evs
}

// 持续背压：下游不确认时，运行时被阻塞、缓冲有界；恢复确认后输出有序且无重复。
func TestBackpressureBoundedAndOrdered(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	sink := NewSink(2) // 极小缓冲，迫使背压
	rt := New(NewSliceSource(backpressureEvents()), 10, 5, 1<<62, sink, NewStore(t.TempDir()))

	done := make(chan error, 1)
	go func() { done <- rt.Run(ctx) }()

	// 下游持续不确认：缓冲被填满后运行时阻塞，输入读取被节制。
	waitFor(t, func() bool { return sink.Pending() == 2 })
	select {
	case err := <-done:
		t.Fatalf("run finished despite downstream overload: %v", err)
	case <-time.After(100 * time.Millisecond):
		t.Logf("backpressure active: pending=%d (cap=2), input throttled", sink.Pending())
	}
	// 内存有界：未确认缓冲不会超过容量。
	if p := sink.Pending(); p > 2 {
		t.Fatalf("buffer unbounded: pending=%d", p)
	}

	// 下游恢复：逐批确认，运行时继续推进。
	var lastSeq int64 = -1
	verified := 0 // 已校验过的确认结果条数（Confirmed 为累计视图）
	var runErr error
	finished := false
	for !finished {
		confirmed := sink.Confirmed()
		for _, r := range confirmed[verified:] {
			if r.Seq <= lastSeq {
				t.Fatalf("duplicate or out-of-order seq: got %d after %d", r.Seq, lastSeq)
			}
			lastSeq = r.Seq
		}
		verified = len(confirmed)
		if p := sink.Pending(); p > 0 {
			sink.ConfirmUpTo(lastSeq + int64(p))
		}
		select {
		case runErr = <-done:
			finished = true
		case <-time.After(10 * time.Millisecond):
		}
	}
	if runErr != nil {
		t.Fatalf("run: %v", runErr)
	}
	// 可见性语义一致：运行结束后全部结果已确认可见。
	got := sink.Confirmed()
	if len(got) != 9 {
		t.Fatalf("expected 9 confirmed results, got %d", len(got))
	}
	for i := 1; i < len(got); i++ {
		if got[i].Seq != got[i-1].Seq+1 {
			t.Fatalf("seq gap/dup at %d: %+v", i, got)
		}
	}
	t.Logf("recovered: %d results confirmed in order, lastSeq=%d", len(got), got[len(got)-1].Seq)
}

// 背压下的取消：ctx 取消时写阻塞必须可中断，不泄漏。
func TestBackpressureCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	sink := NewSink(1)
	rt := New(NewSliceSource(backpressureEvents()), 10, 5, 1<<62, sink, NewStore(t.TempDir()))

	done := make(chan error, 1)
	go func() { done <- rt.Run(ctx) }()
	waitFor(t, func() bool { return sink.Pending() == 1 })
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatalf("expected cancellation error")
		}
		t.Logf("run interrupted under backpressure: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatalf("run did not unblock after cancel")
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("condition not met within deadline")
}
