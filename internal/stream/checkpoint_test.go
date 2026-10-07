package stream

import (
	"context"
	"errors"
	"os"
	"testing"
)

func recoveryEvents() []Event {
	return []Event{
		{ID: "r1", Key: "a", EventTime: 1, Value: 1},
		{ID: "r2", Key: "b", EventTime: 3, Value: 2},
		{ID: "r3", Key: "a", EventTime: 8, Value: 3},
		{ID: "r4", Key: "a", EventTime: 12, Value: 4}, // 关闭 [0,10)
		{ID: "r5", Key: "b", EventTime: 15, Value: 5},
		{ID: "r6", Key: "a", EventTime: 21, Value: 6}, // 关闭 [10,20)
		{ID: "r7", Key: "c", EventTime: 22, Value: 7},
		{ID: "r8", Key: "a", EventTime: 35, Value: 8}, // 关闭 [20,30)
	}
}

// 中断恢复：从已确认检查点重启后，输出与一次性运行完全一致（不重复、不丢失）。
func TestCheckpointRecoveryConsistency(t *testing.T) {
	ctx := context.Background()

	// 基准：一次性跑完。
	baseSink := NewSink(16)
	base := New(NewSliceSource(recoveryEvents()), 10, 5, 1<<62, baseSink, NewStore(t.TempDir()))
	if err := base.Run(ctx); err != nil {
		t.Fatalf("baseline run: %v", err)
	}
	want := baseSink.Confirmed()

	// 故障注入：处理 3 条后检查点，再处理 3 条（含未确认输出）后“崩溃”。
	dir := t.TempDir()
	sink := NewSink(16)
	src := NewSliceSource(recoveryEvents())
	rt := New(src, 10, 5, 1<<62, sink, NewStore(dir))
	for i := 0; i < 3; i++ {
		if _, err := rt.Step(ctx); err != nil {
			t.Fatalf("step: %v", err)
		}
	}
	if err := rt.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	cpOffset := src.Offset()
	cpSeq := rt.NextSeq()
	t.Logf("checkpoint confirmed: sourceOffset=%d nextSeq=%d watermark=%d", cpOffset, cpSeq, rt.Watermark())
	for i := 0; i < 3; i++ {
		if _, err := rt.Step(ctx); err != nil {
			t.Fatalf("step after checkpoint: %v", err)
		}
	}
	if sink.Pending() == 0 {
		t.Fatalf("expected unconfirmed pending results at crash")
	}
	t.Logf("crash injected at sourceOffset=%d (unconfirmed pending=%d)", src.Offset(), sink.Pending())

	// 重启：新运行时共享源、Sink 与存储，从检查点恢复。
	rt2 := New(src, 10, 5, 1<<62, sink, NewStore(dir))
	if err := rt2.Restore(context.Background()); err != nil {
		t.Fatalf("restore: %v", err)
	}
	t.Logf("restored: sourceOffset=%d nextSeq=%d watermark=%d", src.Offset(), rt2.NextSeq(), rt2.Watermark())
	if src.Offset() != cpOffset {
		t.Fatalf("offset not restored: got %d want %d", src.Offset(), cpOffset)
	}
	if rt2.NextSeq() != cpSeq {
		t.Fatalf("nextSeq not restored: got %d want %d", rt2.NextSeq(), cpSeq)
	}
	if err := rt2.Run(ctx); err != nil {
		t.Fatalf("resumed run: %v", err)
	}

	got := sink.Confirmed()
	if len(got) != len(want) {
		t.Fatalf("result count mismatch: got %d want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("result %d mismatch: got %+v want %+v", i, got[i], want[i])
		}
	}
	// 输出 Seq 严格递增（无重复、无乱序）。
	for i := 1; i < len(got); i++ {
		if got[i].Seq <= got[i-1].Seq {
			t.Fatalf("seq not increasing at %d: %+v", i, got)
		}
	}
}

// 检查点损坏：校验和不匹配必须被拒绝，且原因可区分。
func TestCheckpointCorrupt(t *testing.T) {
	dir := t.TempDir()
	store := NewStore(dir)
	rt := New(NewSliceSource(recoveryEvents()), 10, 5, 1<<62, NewSink(16), store)
	if err := rt.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	data, err := os.ReadFile(store.Path())
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	data[len(data)/2] ^= 0xFF // 注入损坏
	if err := os.WriteFile(store.Path(), data, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	err = rt.Restore(context.Background())
	if !errors.Is(err, ErrCheckpointCorrupt) {
		t.Fatalf("expected ErrCheckpointCorrupt, got %v", err)
	}
	t.Logf("corrupt checkpoint rejected: %v", err)
}

// 版本不兼容：必须被拒绝，且与损坏原因可区分。
func TestCheckpointVersionIncompatible(t *testing.T) {
	dir := t.TempDir()
	store := NewStore(dir)
	rt := New(NewSliceSource(recoveryEvents()), 10, 5, 1<<62, NewSink(16), store)
	if err := rt.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	cp, err := store.Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	cp.Version = CheckpointVersion + 1
	if err := store.Save(cp); err != nil {
		t.Fatalf("save: %v", err)
	}
	err = rt.Restore(context.Background())
	if !errors.Is(err, ErrCheckpointVersion) {
		t.Fatalf("expected ErrCheckpointVersion, got %v", err)
	}
	if errors.Is(err, ErrCheckpointCorrupt) {
		t.Fatalf("version error must be distinguishable from corruption: %v", err)
	}
	t.Logf("incompatible version rejected: %v", err)
}

// 位点回退：检查点落后于已确认输出，或源位点越界，均拒绝恢复。
func TestCheckpointOffsetRegression(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store := NewStore(dir)
	sink := NewSink(16)
	src := NewSliceSource(recoveryEvents())
	rt := New(src, 10, 5, 1<<62, sink, store)

	// 跑完并确认全部输出。
	if err := rt.Run(ctx); err != nil {
		t.Fatalf("run: %v", err)
	}
	confirmedSeq := sink.ConfirmedSeq()
	t.Logf("confirmed up to seq=%d", confirmedSeq)

	// 构造一个更旧的检查点（NextSeq 落后于已确认输出）。
	stale := Checkpoint{
		Version:      CheckpointVersion,
		SourceOffset: 2,
		NextSeq:      1,
		Windows:      map[int64]map[string]WindowState{},
	}
	if err := store.Save(stale); err != nil {
		t.Fatalf("save stale: %v", err)
	}
	rt2 := New(src, 10, 5, 1<<62, sink, store)
	err := rt2.Restore(context.Background())
	if !errors.Is(err, ErrOffsetRegression) {
		t.Fatalf("expected ErrOffsetRegression, got %v", err)
	}
	t.Logf("stale checkpoint rejected: %v", err)

	// 源位点越界（超出可重放范围）同样拒绝。
	future := Checkpoint{
		Version:      CheckpointVersion,
		SourceOffset: 1000,
		NextSeq:      confirmedSeq + 1,
		Windows:      map[int64]map[string]WindowState{},
	}
	if err := store.Save(future); err != nil {
		t.Fatalf("save future: %v", err)
	}
	err = rt2.Restore(context.Background())
	if !errors.Is(err, ErrOffsetRegression) {
		t.Fatalf("expected ErrOffsetRegression for out-of-range offset, got %v", err)
	}
	t.Logf("out-of-range offset rejected: %v", err)
}
