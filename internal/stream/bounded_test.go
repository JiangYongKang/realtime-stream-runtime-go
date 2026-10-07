package stream

import (
	"context"
	"strconv"
	"testing"
	"time"
)

// longEvents 生成从 eventTime=1 起、每条推进 1ms 的 n 条唯一事件。
func longEvents(n int) []Event {
	evs := make([]Event, 0, n)
	for i := 0; i < n; i++ {
		evs = append(evs, Event{
			ID:        "L" + strconv.Itoa(i),
			Key:       "k" + strconv.Itoa(i%50),
			EventTime: int64(i + 1),
			Value:     1,
		})
	}
	return evs
}

// 长时间运行：去重身份集合与结果历史随保留范围有界，不随运行时间无限增长；
// 落在保留范围内的重复仍被识别、保留范围外的历史结果被回收。
func TestBoundedStateUnderRetention(t *testing.T) {
	ctx := context.Background()
	const (
		n         = 5000
		retention = 200
		winSize   = 10
	)
	events := longEvents(n)
	sink := NewSink(256) // 小缓冲即可：后台持续确认，本用例只观测去重/历史状态，不验证背压
	rt := New(NewSliceSource(events), winSize, 0, retention, sink, NewStore(t.TempDir()))
	stopDrain := make(chan struct{})
	go func() {
		for {
			select {
			case <-stopDrain:
				return
			default:
				if p := sink.Pending(); p > 0 {
					sink.ConfirmUpTo(sink.ConfirmedSeq() + int64(p))
				}
				time.Sleep(time.Millisecond)
			}
		}
	}()
	if err := rt.Run(ctx); err != nil {
		close(stopDrain)
		t.Fatalf("run: %v", err)
	}
	close(stopDrain)

	// 去重身份只保留最后约 retention 个事件时间，条目数远小于总输入，且有确定上界。
	dc := rt.DedupCount()
	if dc >= n/4 {
		t.Fatalf("dedup state grew unbounded: count=%d of %d events", dc, n)
	}
	if dc > retention+pruneInterval+50 {
		t.Fatalf("dedup state exceeds retention bound: count=%d retention=%d", dc, retention)
	}
	// 历史结果只保留窗口结束时间在保留范围内的尾部，数量有界。
	hc := rt.HistoryCount()
	if hc >= n/4 {
		t.Fatalf("history grew unbounded: count=%d of %d outputs", hc, n)
	}
	t.Logf("after %d events: dedupEntries=%d historyResults=%d (retention=%dms) watermark=%d",
		n, dc, hc, retention, rt.Watermark())

	// 落在保留范围内的重放必须照样去重：拿最后处理过的一条事件再喂一次。
	recent := events[n-5]
	src := NewSliceSource([]Event{recent})
	rt2 := New(src, winSize, 0, retention, NewSink(8), NewStore(t.TempDir()))
	// 直接注入与长跑末尾一致的去重身份集合，验证保留范围内的命中。
	rt2.dedup.restore(rt.dedup.snapshot())
	done, err := rt2.Step(ctx)
	if err != nil || done {
		t.Fatalf("step recent dup: done=%v err=%v", done, err)
	}
	if len(rt2.Duplicates) != 1 {
		t.Fatalf("event within retention must still dedup: duplicates=%d", len(rt2.Duplicates))
	}
	t.Logf("in-retention replay still deduped: id=%s eventTime=%d", recent.ID, recent.EventTime)
}

// 空下游持续不消费时做恢复：历史结果重发受有界 Sink 背压节制，
// 未确认缓冲绝不超过容量，内存不被撑爆；消费恢复后完整结果与基线一致。
func TestBoundedMemoryRestoreIntoStalledSink(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	events := longEvents(3000)
	dir := t.TempDir()

	// 第一段：跑到结束并留下检查点 + 提交高水位。
	rt1 := New(NewSliceSource(events), 10, 0, 1<<62, NewSink(4096), NewStore(dir))
	if err := rt1.Run(ctx); err != nil {
		t.Fatalf("first run: %v", err)
	}

	// 全新进程 + 空下游（容量极小且无人消费），恢复重放必须被背压挡住、缓冲有界。
	stalled := NewSink(2)
	rt2 := New(NewSliceSource(events), 10, 0, 1<<62, stalled, NewStore(dir))
	done := make(chan error, 1)
	go func() { done <- rt2.Restore(ctx) }()

	waitFor(t, func() bool { return stalled.Pending() == 2 })
	select {
	case err := <-done:
		t.Fatalf("restore completed despite stalled downstream: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	if p := stalled.Pending(); p > 2 {
		t.Fatalf("restore replay unbounded under stalled sink: pending=%d", p)
	}
	t.Logf("restore replay back-pressured into empty stalled sink: pending=%d (cap=2), input throttled",
		stalled.Pending())

	// 下游恢复消费：逐批确认直到恢复完成，结果必须与基线逐条一致且不重不漏。
	baseSink := NewSink(4096)
	base := New(NewSliceSource(events), 10, 0, 1<<62, baseSink, NewStore(t.TempDir()))
	if err := base.Run(ctx); err != nil {
		t.Fatalf("baseline: %v", err)
	}
	want := baseSink.Confirmed()

	finished := false
	for !finished {
		p := stalled.Pending()
		if p > 0 {
			stalled.ConfirmUpTo(stalled.ConfirmedSeq() + int64(p))
		}
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("restore: %v", err)
			}
			finished = true
		case <-time.After(time.Millisecond):
		}
	}
	got := stalled.Confirmed()
	if len(got) != len(want) {
		t.Fatalf("restored output count mismatch: got %d want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("restored result %d mismatch: got %+v want %+v", i, got[i], want[i])
		}
	}
	t.Logf("downstream drained: %d historical results re-visible in order, lastSeq=%d",
		len(got), got[len(got)-1].Seq)
}
