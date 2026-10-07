package stream

import (
	"context"
	"testing"
)

// uniqueEvents 是完全不含重复的基线输入（每窗口 2 个 key）。
func uniqueEvents() []Event {
	return []Event{
		{ID: "e1", Key: "a", EventTime: 1, Value: 1},
		{ID: "e2", Key: "b", EventTime: 2, Value: 10},
		{ID: "e3", Key: "a", EventTime: 11, Value: 2},
		{ID: "e4", Key: "b", EventTime: 12, Value: 20},
		{ID: "e5", Key: "a", EventTime: 25, Value: 3}, // maxSeen=25,wm=20 关闭 [0,10)[10,20)
	}
}

func runClean(t *testing.T, evs []Event) ([]Result, *Runtime) {
	t.Helper()
	sink := NewSink(64)
	rt := New(NewSliceSource(evs), 10, 5, 1<<62, sink, NewStore(t.TempDir()))
	if err := rt.Run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	return sink.Confirmed(), rt
}

// 重复投递在结果层面幂等：同一批输入中重复多次、与正常事件任意顺序混排，
// 每个窗口每个 key 的聚合都与完全不重复时一致；重复被单独统计。
func TestDuplicateIdempotent(t *testing.T) {
	ctx := context.Background()
	base, baseRT := runClean(t, uniqueEvents())

	// 构造含重复的输入：开头先到副本、中间夹重复、结尾再补重复，顺序任意打乱。
	withDup := []Event{
		{ID: "e3", Key: "a", EventTime: 11, Value: 2}, // 重复事件先到
		{ID: "e1", Key: "a", EventTime: 1, Value: 1},
		{ID: "e1", Key: "a", EventTime: 1, Value: 1}, // 同批连续重复
		{ID: "e2", Key: "b", EventTime: 2, Value: 10},
		{ID: "e4", Key: "b", EventTime: 12, Value: 20},
		{ID: "e2", Key: "b", EventTime: 2, Value: 10}, // 与正常事件混排的重复
		{ID: "e3", Key: "a", EventTime: 11, Value: 2}, // 再次重复
		{ID: "e5", Key: "a", EventTime: 25, Value: 3},
	}
	got, rt := runClean(t, withDup)

	if len(got) != len(base) {
		t.Fatalf("result count differs: got %d want %d", len(got), len(base))
	}
	for i := range base {
		if got[i] != base[i] {
			t.Fatalf("result %d mismatch with duplicates: got %+v want %+v", i, got[i], base[i])
		}
	}
	// e1 出现 2 次、e2 出现 2 次、e3 出现 2 次 -> 3 条重复被识别。
	if len(rt.Duplicates) != 3 {
		t.Fatalf("expected 3 duplicates, got %d", len(rt.Duplicates))
	}
	t.Logf("duplicates suppressed=%d (results identical to clean run), last eventTime=%d watermark=%d",
		len(rt.Duplicates), 25, rt.Watermark())

	// 重复统计与数据错误、超范围迟到完全分开：这里后两者必须为 0。
	if len(rt.DataErrors) != 0 || len(rt.LateDropped) != 0 {
		t.Fatalf("duplicates must not be mixed into errors/late: errors=%d late=%d",
			len(rt.DataErrors), len(rt.LateDropped))
	}
	// 基线运行也没有任何重复。
	if len(baseRT.Duplicates) != 0 {
		t.Fatalf("clean run should see no duplicates, got %d", len(baseRT.Duplicates))
	}
	_ = ctx
}

// 一条已经处理过的事件若在窗口关闭后重放，仍计为重复，而不是超范围迟到。
func TestLateDuplicateCountedAsDuplicate(t *testing.T) {
	events := []Event{
		{ID: "x1", Key: "a", EventTime: 1, Value: 1},
		{ID: "x2", Key: "a", EventTime: 25, Value: 2}, // 关闭 [0,10)，wm=20
		{ID: "x1", Key: "a", EventTime: 1, Value: 1},  // 窗口已关闭，但它是重复投递
	}
	got, rt := runClean(t, events)
	var closed *Result
	for i := range got {
		if got[i].WindowStart == 0 {
			closed = &got[i]
		}
	}
	if closed == nil || closed.Count != 1 || closed.Sum != 1 {
		t.Fatalf("duplicate must not change closed-window result: %+v", got)
	}
	if len(rt.Duplicates) != 1 {
		t.Fatalf("expected 1 duplicate, got %d", len(rt.Duplicates))
	}
	if len(rt.LateDropped) != 0 {
		t.Fatalf("late duplicate must count as duplicate not late-drop: %d", len(rt.LateDropped))
	}
	t.Logf("late replay of known event counted as duplicate: id=%s eventTime=%d",
		rt.Duplicates[0].Event.ID, rt.Duplicates[0].Event.EventTime)
}

// 缺失身份（空 ID）属于数据错误，不能被当成重复也不能静默吞掉。
func TestEmptyIDIsDataError(t *testing.T) {
	events := []Event{
		{ID: "", Key: "a", EventTime: 1, Value: 1},
	}
	_, rt := runClean(t, events)
	if len(rt.DataErrors) != 1 {
		t.Fatalf("expected 1 data error for empty id, got %d", len(rt.DataErrors))
	}
	if len(rt.Duplicates) != 0 {
		t.Fatalf("empty id must not be treated as duplicate: %d", len(rt.Duplicates))
	}
}
