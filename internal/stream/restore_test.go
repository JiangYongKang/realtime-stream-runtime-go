package stream

import (
	"context"
	"errors"
	"os"
	"testing"
)

// 全新进程 + 空下游恢复：手里只有落盘检查点与同一份可重放输入。
// 恢复后历史结果重新可见，后续结果确定接上；整体输出与从头不中断跑的基线逐条一致。
func TestFreshProcessEmptySinkRecovery(t *testing.T) {
	ctx := context.Background()
	events := recoveryEvents()

	// 基线：从头不中断跑一遍。
	baseSink := NewSink(16)
	base := New(NewSliceSource(events), 10, 5, 1<<62, baseSink, NewStore(t.TempDir()))
	if err := base.Run(ctx); err != nil {
		t.Fatalf("baseline run: %v", err)
	}
	want := baseSink.Confirmed()

	// 第一个进程：处理 3 条后检查点（已对外可见），再处理几条后崩溃。
	dir := t.TempDir()
	src1 := NewSliceSource(events)
	sink1 := NewSink(16)
	rt1 := New(src1, 10, 5, 1<<62, sink1, NewStore(dir))
	for i := 0; i < 3; i++ {
		if _, err := rt1.Step(ctx); err != nil {
			t.Fatalf("step: %v", err)
		}
	}
	if err := rt1.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	t.Logf("checkpoint: sourceOffset=%d nextSeq=%d watermark=%d (confirmed visible to old downstream)",
		src1.Offset(), rt1.NextSeq(), rt1.Watermark())
	for i := 0; i < 2; i++ {
		if _, err := rt1.Step(ctx); err != nil {
			t.Fatalf("step after checkpoint: %v", err)
		}
	}
	t.Logf("--- process replaced: new process, brand-new EMPTY downstream ---")

	// 全新进程：全新的可重放源、全新的空 Sink，只有落盘检查点还在 dir。
	src2 := NewSliceSource(events)
	sink2 := NewSink(16) // 空下游
	rt2 := New(src2, 10, 5, 1<<62, sink2, NewStore(dir))
	if err := rt2.Restore(ctx); err != nil {
		t.Fatalf("restore into fresh process: %v", err)
	}
	t.Logf("restored&replayed: sourceOffset=%d nextSeq=%d watermark=%d confirmed=%d",
		src2.Offset(), rt2.NextSeq(), rt2.Watermark(), len(sink2.Confirmed()))

	got := sink2.Confirmed()
	if len(got) != len(want) {
		t.Fatalf("fresh-process output count mismatch: got %d want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("fresh-process result %d mismatch: got %+v want %+v", i, got[i], want[i])
		}
	}
	for i := 1; i < len(got); i++ {
		if got[i].Seq != got[i-1].Seq+1 {
			t.Fatalf("seq gap/dup at %d: %+v", i, got)
		}
	}
	t.Logf("history re-visible to empty downstream: %d results, lastSeq=%d", len(got), got[len(got)-1].Seq)
}

// 进度回退：空下游恢复到一个更旧的检查点必须被拒绝（旧检查点的 NextSeq 落后于
// 最近一次已确认进度）。这里用“先跑完全量、再写入更旧检查点”的场景验证拒绝。
func TestFreshProcessRejectsOlderProgress(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	events := recoveryEvents()

	store := NewStore(dir)
	rt := New(NewSliceSource(events), 10, 5, 1<<62, NewSink(16), store)
	if err := rt.Run(ctx); err != nil {
		t.Fatalf("run: %v", err)
	}
	latestSeq := rt.NextSeq()
	t.Logf("latest confirmed progress nextSeq=%d", latestSeq)

	// 写入一个更旧的检查点：offset/nextSeq 都落后。
	stale := Checkpoint{
		Version:      CheckpointVersion,
		SourceOffset: 1,
		NextSeq:      0, // 没有任何已确认结果
		Results:      nil,
		Windows:      map[int64]map[string]WindowState{},
	}
	if err := store.Save(stale); err != nil {
		t.Fatalf("save stale: %v", err)
	}
	// 全新进程 + 空下游：虽然下游为空，但重放结果会落到比最近确认更旧的进度，
	// 重放后与检查点比较检测到回退，拒绝继续。
	fresh := New(NewSliceSource(events), 10, 5, 1<<62, NewSink(16), store)
	err := fresh.Restore(ctx)
	if !errors.Is(err, ErrOffsetRegression) {
		t.Fatalf("expected ErrOffsetRegression for older progress, got %v", err)
	}
	t.Logf("older-progress recovery rejected into empty downstream: %v", err)

	// 非空下游：已确认输出领先于检查点时直接拒绝（不做任何重放）。
	dir2 := t.TempDir()
	store2 := NewStore(dir2)
	sink := NewSink(16)
	rtA := New(NewSliceSource(events), 10, 5, 1<<62, sink, store2)
	if err := rtA.Run(ctx); err != nil {
		t.Fatalf("run A: %v", err)
	}
	confirmedSeq := sink.ConfirmedSeq()
	stale2 := Checkpoint{Version: CheckpointVersion, SourceOffset: 0, NextSeq: 1}
	if err := store2.Save(stale2); err != nil {
		t.Fatalf("save stale2: %v", err)
	}
	rtB := New(NewSliceSource(events), 10, 5, 1<<62, sink, store2)
	err = rtB.Restore(ctx)
	if !errors.Is(err, ErrOffsetRegression) {
		t.Fatalf("expected ErrOffsetRegression against non-empty downstream, got %v", err)
	}
	t.Logf("regression against downstream confirmedSeq=%d rejected: %v", confirmedSeq, err)
}

// 落盘检查点损坏或代际不兼容，全新进程恢复必须明确拒绝，且原因可区分。
func TestFreshProcessRejectsCorruptAndGeneration(t *testing.T) {
	events := recoveryEvents()

	t.Run("corrupt", func(t *testing.T) {
		dir := t.TempDir()
		store := NewStore(dir)
		rt := New(NewSliceSource(events), 10, 5, 1<<62, NewSink(16), store)
		if err := rt.Checkpoint(); err != nil {
			t.Fatalf("checkpoint: %v", err)
		}
		data, err := os.ReadFile(store.Path())
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		data[len(data)/2] ^= 0xFF
		if err := os.WriteFile(store.Path(), data, 0o644); err != nil {
			t.Fatalf("write: %v", err)
		}
		fresh := New(NewSliceSource(events), 10, 5, 1<<62, NewSink(16), store)
		err = fresh.Restore(context.Background())
		if !errors.Is(err, ErrCheckpointCorrupt) {
			t.Fatalf("expected ErrCheckpointCorrupt, got %v", err)
		}
		t.Logf("corrupt checkpoint rejected on fresh process: %v", err)
	})

	t.Run("incompatible_generation", func(t *testing.T) {
		dir := t.TempDir()
		store := NewStore(dir)
		rt := New(NewSliceSource(events), 10, 5, 1<<62, NewSink(16), store)
		if err := rt.Checkpoint(); err != nil {
			t.Fatalf("checkpoint: %v", err)
		}
		cp, err := store.Load()
		if err != nil {
			t.Fatalf("load: %v", err)
		}
		cp.Version = CheckpointVersion + 1 // 下一代格式
		if err := store.Save(cp); err != nil {
			t.Fatalf("save: %v", err)
		}
		fresh := New(NewSliceSource(events), 10, 5, 1<<62, NewSink(16), store)
		err = fresh.Restore(context.Background())
		if !errors.Is(err, ErrCheckpointVersion) {
			t.Fatalf("expected ErrCheckpointVersion, got %v", err)
		}
		if errors.Is(err, ErrCheckpointCorrupt) {
			t.Fatalf("generation mismatch must be distinguishable from corrupt: %v", err)
		}
		t.Logf("incompatible generation rejected on fresh process: %v", err)
	})

	t.Run("structural_mismatch", func(t *testing.T) {
		dir := t.TempDir()
		store := NewStore(dir)
		bad := Checkpoint{
			Version:      CheckpointVersion,
			SourceOffset: 3,
			NextSeq:      5,
			Results: []Result{
				{WindowStart: 0, WindowEnd: 10, Key: "a", Seq: 0},
				{WindowStart: 10, WindowEnd: 20, Key: "a", Seq: 2}, // 序号不连续
			},
		}
		if err := store.Save(bad); err != nil {
			t.Fatalf("save bad: %v", err)
		}
		fresh := New(NewSliceSource(events), 10, 5, 1<<62, NewSink(16), store)
		err := fresh.Restore(context.Background())
		if !errors.Is(err, ErrCheckpointCorrupt) {
			t.Fatalf("structural mismatch should be ErrCheckpointCorrupt, got %v", err)
		}
		t.Logf("internally inconsistent checkpoint rejected: %v", err)
	})
}
