package stream

import (
	"context"
	"errors"
	"testing"
)

// 窗口宽度 10ms，允许迟到 5ms：watermark = maxEventTime - 5。
// 窗口 [10,20) 在 watermark >= 20 时关闭。
func TestWindowAggregationAndLateness(t *testing.T) {
	dir := t.TempDir()
	sink := NewSink(16)
	src := NewSliceSource([]Event{
		{ID: "w1", Key: "a", EventTime: 12, Value: 1},  // 窗口 [10,20)
		{ID: "w2", Key: "a", EventTime: 22, Value: 2},  // 窗口 [20,30)，maxSeen=22, wm=17
		{ID: "w3", Key: "a", EventTime: 14, Value: 10}, // 允许范围内迟到：窗口 [10,20) 未关闭，归并
		{ID: "w4", Key: "a", EventTime: 25, Value: 3},  // maxSeen=25, wm=20 -> 关闭 [10,20)
		{ID: "w5", Key: "a", EventTime: 11, Value: 99}, // 超范围迟到：窗口 [10,20) 已关闭，丢弃并记录
		{ID: "bad", Key: "", EventTime: 30, Value: 1},  // 数据错误：空 key，与迟到区分
	})
	rt := New(src, 10, 5, 1<<62, sink, NewStore(dir))
	ctx := context.Background()

	for i := 0; i < 6; i++ {
		done, err := rt.Step(ctx)
		if err != nil {
			t.Fatalf("step %d: %v", i, err)
		}
		if done {
			t.Fatalf("step %d: unexpected done", i)
		}
		t.Logf("step=%d watermark=%d nextSeq=%d", i, rt.Watermark(), rt.NextSeq())
	}

	confirmed := sinkConfirmedAfterCheckpoint(t, rt)
	// 第一个窗口 [10,20)：事件 12(v=1) 与迟到事件 14(v=10) 归并 -> count=2, sum=11。
	if len(confirmed) != 1 {
		t.Fatalf("expected 1 emitted result, got %d", len(confirmed))
	}
	r := confirmed[0]
	if r.WindowStart != 10 || r.WindowEnd != 20 || r.Count != 2 || r.Sum != 11 {
		t.Fatalf("unexpected result: %+v", r)
	}
	if r.Seq != 0 {
		t.Fatalf("expected seq 0, got %d", r.Seq)
	}

	// 超范围迟到事件被明确记录。
	if len(rt.LateDropped) != 1 {
		t.Fatalf("expected 1 late drop, got %d", len(rt.LateDropped))
	}
	ld := rt.LateDropped[0]
	if ld.Event.EventTime != 11 || ld.WindowStart != 10 || ld.Watermark != 20 {
		t.Fatalf("unexpected late record: %+v", ld)
	}
	t.Logf("late drop recorded: eventTime=%d window=[%d,%d) watermark=%d reason=%q",
		ld.Event.EventTime, ld.WindowStart, ld.WindowEnd, ld.Watermark, ld.Reason)

	// 数据错误与迟到丢弃可区分。
	if len(rt.DataErrors) != 1 || !errors.Is(rt.DataErrors[0], ErrDataCorrupt) {
		t.Fatalf("expected 1 data error, got %v", rt.DataErrors)
	}
}

// 正常乱序但不超范围：结果与顺序输入一致（确定性归并）。
func TestOutOfOrderDeterministic(t *testing.T) {
	mk := func(events []Event) []Result {
		sink := NewSink(16)
		rt := New(NewSliceSource(events), 10, 5, 1<<62, sink, NewStore(t.TempDir()))
		if err := rt.Run(context.Background()); err != nil {
			t.Fatalf("run: %v", err)
		}
		return sink.Confirmed()
	}
	inOrder := []Event{
		{ID: "o1", Key: "a", EventTime: 1, Value: 1},
		{ID: "o2", Key: "a", EventTime: 5, Value: 2},
		{ID: "o3", Key: "b", EventTime: 7, Value: 3},
		{ID: "o4", Key: "a", EventTime: 30, Value: 4},
	}
	outOfOrder := []Event{
		{ID: "o2", Key: "a", EventTime: 5, Value: 2},
		{ID: "o3", Key: "b", EventTime: 7, Value: 3},
		{ID: "o1", Key: "a", EventTime: 1, Value: 1},
		{ID: "o4", Key: "a", EventTime: 30, Value: 4},
	}
	a, b := mk(inOrder), mk(outOfOrder)
	if len(a) != len(b) {
		t.Fatalf("result count differs: %d vs %d", len(a), len(b))
	}
	for i := range a {
		// Seq 由发射顺序决定，比较窗口内容。
		if a[i].WindowStart != b[i].WindowStart || a[i].Key != b[i].Key ||
			a[i].Count != b[i].Count || a[i].Sum != b[i].Sum {
			t.Fatalf("result %d differs: %+v vs %+v", i, a[i], b[i])
		}
	}
}

func sinkConfirmedAfterCheckpoint(t *testing.T, rt *Runtime) []Result {
	t.Helper()
	if err := rt.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	return rt.sink.Confirmed()
}
