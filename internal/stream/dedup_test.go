package stream

import (
	"context"
	"errors"
	"testing"
)

// dedupBaseline 返回不带任何重复的输入，作为聚合基线。
func dedupBaseline() []Event {
	return []Event{
		{ID: "e1", Key: "a", EventTime: 1, Value: 1},
		{ID: "e2", Key: "a", EventTime: 8, Value: 3},
		{ID: "e3", Key: "b", EventTime: 3, Value: 2},
		{ID: "e4", Key: "a", EventTime: 12, Value: 4}, // watermark>=10 关闭 [0,10)
		{ID: "e5", Key: "b", EventTime: 15, Value: 5},
		{ID: "e6", Key: "a", EventTime: 21, Value: 6}, // 关闭 [10,20)
	}
}

func runWithDupStats(t *testing.T, evs []Event, retention int64) ([]Result, *Runtime) {
	t.Helper()
	sink := NewSink(64)
	rt := NewWithConfig(NewSliceSource(evs), Config{
		WindowSize: 10, AllowedLateness: 5, DedupRetention: retention, Generation: "g1",
	}, sink, NewStore(t.TempDir()))
	if err := rt.Run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	return sink.Confirmed(), rt
}

func baselineResults(t *testing.T) []Result {
	t.Helper()
	res, _ := runWithDupStats(t, dedupBaseline(), 0)
	return res
}

// 同批输入中重复事件紧挨出现、以及与正常事件任意乱序混在一起，
// 每个窗口每个 key 的聚合都必须与完全无重复的基线一致，且重复被单独统计。
func TestDuplicateIdempotent(t *testing.T) {
	want := baselineResults(t)

	withDup := []Event{
		{ID: "e1", Key: "a", EventTime: 1, Value: 1},
		{ID: "e1", Key: "a", EventTime: 1, Value: 1}, // 紧接重复
		{ID: "e2", Key: "a", EventTime: 8, Value: 3},
		{ID: "e3", Key: "b", EventTime: 3, Value: 2},
		{ID: "e2", Key: "a", EventTime: 8, Value: 3}, // 乱序后重复
		{ID: "e1", Key: "a", EventTime: 1, Value: 1}, // 再次重复
		{ID: "e4", Key: "a", EventTime: 12, Value: 4},
		{ID: "e5", Key: "b", EventTime: 15, Value: 5},
		{ID: "e6", Key: "a", EventTime: 21, Value: 6},
		{ID: "e3", Key: "b", EventTime: 3, Value: 2}, // 落在关闭边界前的迟到重复
	}
	got, rt := runWithDupStats(t, withDup, 0)

	if len(got) != len(want) {
		t.Fatalf("result count mismatch: got %d want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("result %d mismatch:\n got %+v\nwant %+v", i, got[i], want[i])
		}
	}
	t.Logf("idempotent aggregation: %d windows identical to dedup-free baseline", len(got))

	// 4 次重复（e1 x2、e2、e3）被单独识别统计。
	if len(rt.Duplicates) != 4 {
		t.Fatalf("expected 4 duplicates, got %d: %+v", len(rt.Duplicates), rt.Duplicates)
	}
	for _, d := range rt.Duplicates {
		t.Logf("duplicate suppressed: id=%s key=%s eventTime=%d watermark=%d",
			d.Event.ID, d.Event.Key, d.Event.EventTime, d.Watermark)
	}

	// 重复不能与数据错误、超范围迟到混算。
	if len(rt.DataErrors) != 0 {
		t.Fatalf("expected no data errors, got %v", rt.DataErrors)
	}
	if len(rt.LateDropped) != 0 {
		t.Fatalf("expected no late drops, got %+v", rt.LateDropped)
	}
}

// 同 ID 但内容冲突属于数据错误，不能被当成重复静默吞掉，也不能计入聚合。
func TestDuplicateConflictIsDataError(t *testing.T) {
	evs := []Event{
		{ID: "e1", Key: "a", EventTime: 1, Value: 1},
		{ID: "e1", Key: "a", EventTime: 1, Value: 999}, // 同 ID 不同 value
		{ID: "e2", Key: "a", EventTime: 2, Value: 2},
	}
	sink := NewSink(16)
	rt := NewWithConfig(NewSliceSource(evs), Config{
		WindowSize: 10, AllowedLateness: 5, Generation: "g1",
	}, sink, NewStore(t.TempDir()))
	if err := rt.Run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(rt.Duplicates) != 0 {
		t.Fatalf("conflict must not count as duplicate: %+v", rt.Duplicates)
	}
	if len(rt.DataErrors) != 1 || !errors.Is(rt.DataErrors[0], ErrDataCorrupt) {
		t.Fatalf("expected 1 ErrDataCorrupt, got %v", rt.DataErrors)
	}
	t.Logf("conflicting replay rejected as data error: %v", rt.DataErrors[0])
	// 冲突事件不进入聚合：窗口内只有 e1 与 e2。
	got := sink.Confirmed()
	if len(got) != 1 || got[0].Count != 2 || got[0].Sum != 3 {
		t.Fatalf("conflict event must not aggregate: %+v", got)
	}
}

// 超范围迟到与重复必须可区分：一个真迟到、一个重复，分别落到各自统计里。
func TestDuplicateDistinctFromLateDrop(t *testing.T) {
	evs := []Event{
		{ID: "e1", Key: "a", EventTime: 1, Value: 1},
		{ID: "e2", Key: "a", EventTime: 12, Value: 4}, // wm=7，窗口未关
		{ID: "e3", Key: "a", EventTime: 25, Value: 7}, // wm=20 关闭 [0,10)
		{ID: "e1", Key: "a", EventTime: 1, Value: 1},  // 重复（同内容）
		{ID: "e4", Key: "a", EventTime: 2, Value: 8},  // 真正的超范围迟到
	}
	_, rt := runWithDupStats(t, evs, 0)
	if len(rt.Duplicates) != 1 || rt.Duplicates[0].Event.ID != "e1" {
		t.Fatalf("expected duplicate e1, got %+v", rt.Duplicates)
	}
	if len(rt.LateDropped) != 1 || rt.LateDropped[0].Event.ID != "e4" {
		t.Fatalf("expected late drop e4, got %+v", rt.LateDropped)
	}
	t.Logf("distinct stats: duplicate=%d lateDropped=%d dataErrors=%d",
		len(rt.Duplicates), len(rt.LateDropped), len(rt.DataErrors))
}
