package stream

import (
	"context"
	"errors"
	"testing"
	"time"
)

func recoveryV2Events() []Event {
	evs := recoveryEvents()
	out := make([]Event, len(evs))
	for i, e := range evs {
		e.ID = string(rune('a' + i))
		out[i] = e
	}
	return out
}

func cfgV2() Config {
	return Config{WindowSize: 10, AllowedLateness: 5, DedupRetention: 1000, Generation: "gen-A"}
}

// 全新进程 + 全新空下游：手里只有落盘检查点和可重放输入，恢复后历史结果
// 重新可见，后续结果按确定顺序接上，整体与从头不中断跑的基线逐条一致。
func TestFreshProcessEmptySinkReplay(t *testing.T) {
	ctx := context.Background()
	evs := recoveryV2Events()

	// 基线：从头不中断跑一遍。
	baseSink := NewSink(16)
	base := NewWithConfig(NewSliceSource(evs), cfgV2(), baseSink, NewStore(t.TempDir()))
	if err := base.Run(ctx); err != nil {
		t.Fatalf("baseline: %v", err)
	}
	want := baseSink.Confirmed()

	// 旧进程：处理到一半做检查点，再处理若干条后"被换掉"。
	dir := t.TempDir()
	store := NewStore(dir)
	src := NewSliceSource(evs)
	old := NewWithConfig(src, cfgV2(), NewSink(16), store)
	for i := 0; i < 3; i++ {
		if _, err := old.Step(ctx); err != nil {
			t.Fatalf("step: %v", err)
		}
	}
	if err := old.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	for i := 0; i < 2; i++ {
		if _, err := old.Step(ctx); err != nil {
			t.Fatalf("step post-cp: %v", err)
		}
	}
	t.Logf("old process died: checkpointOffset=%d consumedOffset=%d nextSeq=%d watermark=%d",
		3, src.Offset(), old.NextSeq(), old.Watermark())

	// 新进程：全新 Source 实例、全新空 Sink，只有同一个落盘 Store。
	newSrc := NewSliceSource(evs)
	emptySink := NewSink(16)
	fresh := NewWithConfig(newSrc, cfgV2(), emptySink, NewStore(dir))
	if err := fresh.Restore(); err != nil {
		t.Fatalf("restore: %v", err)
	}
	t.Logf("fresh process restored by full replay: sourceOffset=%d nextSeq=%d (history rebuilt onto empty downstream)",
		newSrc.Offset(), fresh.NextSeq())
	if newSrc.Offset() != 0 {
		t.Fatalf("full replay must seek to 0, got %d", newSrc.Offset())
	}
	if err := fresh.Run(ctx); err != nil {
		t.Fatalf("resumed run: %v", err)
	}

	got := emptySink.Confirmed()
	if len(got) != len(want) {
		t.Fatalf("result count mismatch: got %d want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("result %d mismatch:\n got %+v\nwant %+v", i, got[i], want[i])
		}
	}
	// 不重复、不丢：Seq 严格连续递增。
	for i := int64(1); i < int64(len(got)); i++ {
		if got[i].Seq != got[i-1].Seq+1 {
			t.Fatalf("seq gap/dup at %d: %+v", i, got)
		}
	}
	t.Logf("fresh process output equals baseline: %d results, seq 0..%d", len(got), got[len(got)-1].Seq)
}

// 代际不兼容：换了处理逻辑代际时必须明确拒绝，且与损坏/版本错误可区分。
func TestRestoreRejectsGenerationMismatch(t *testing.T) {
	dir := t.TempDir()
	store := NewStore(dir)
	old := NewWithConfig(NewSliceSource(recoveryV2Events()), cfgV2(), NewSink(16), store)
	if err := old.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	// 新进程配置了不同代际，且下游为空。
	other := cfgV2()
	other.Generation = "gen-B"
	fresh := NewWithConfig(NewSliceSource(recoveryV2Events()), other, NewSink(16), NewStore(dir))
	err := fresh.Restore()
	if !errors.Is(err, ErrGenerationIncompatible) {
		t.Fatalf("expected ErrGenerationIncompatible, got %v", err)
	}
	if errors.Is(err, ErrCheckpointVersion) || errors.Is(err, ErrCheckpointCorrupt) {
		t.Fatalf("generation mismatch must be distinguishable: %v", err)
	}
	t.Logf("generation mismatch rejected: %v", err)
}

// 空下游全量重放期间下游持续不消费：Sink 背压保证缓冲有界、内存不被撑爆。
func TestReplayBackpressureBoundedMemory(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	dir := t.TempDir()
	store := NewStore(dir)
	evs := backpressureEventsWithIDs()
	old := NewWithConfig(NewSliceSource(evs), cfgV2(), NewSink(16), store)
	if err := old.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	// 全新空进程，极小 Sink，且下游一开始完全不消费。
	emptySink := NewSink(2)
	fresh := NewWithConfig(NewSliceSource(evs), cfgV2(), emptySink, NewStore(dir))
	if err := fresh.Restore(); err != nil {
		t.Fatalf("restore: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- fresh.Run(ctx) }()

	waitFor(t, func() bool { return emptySink.Pending() == 2 })
	for i := 0; i < 30; i++ {
		if p := emptySink.Pending(); p > 2 {
			t.Fatalf("replay memory unbounded under non-consuming downstream: pending=%d", p)
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Logf("replay throttled with empty/slow downstream: pending=%d (cap=2), memory bounded",
		emptySink.Pending())

	// 恢复消费后重放继续并完整产出。
	go func() {
		for {
			time.Sleep(5 * time.Millisecond)
			if p := emptySink.Pending(); p > 0 {
				emptySink.ConfirmUpTo(emptySink.ConfirmedSeq() + int64(p))
			}
		}
	}()
	if err := <-done; err != nil {
		t.Fatalf("run: %v", err)
	}
	got := emptySink.Confirmed()
	if len(got) != 9 {
		t.Fatalf("expected 9 results after replay drain, got %d", len(got))
	}
	t.Logf("replay drained after downstream recovered: %d results, lastSeq=%d",
		len(got), got[len(got)-1].Seq)
}

func backpressureEventsWithIDs() []Event {
	base := backpressureEvents()
	out := make([]Event, len(base))
	for i, e := range base {
		e.ID = string(rune('a' + i))
		out[i] = e
	}
	return out
}

// 老格式（v1）检查点必须以版本不兼容明确拒绝，不做静默迁移。
func TestRestoreRejectsLegacyV1(t *testing.T) {
	dir := t.TempDir()
	store := NewStore(dir)
	// 手工构造一份 v1 落盘（不带 generation/seen 的旧字段集合）。
	legacy := Checkpoint{
		Version:      1,
		SourceOffset: 0,
		NextSeq:      0,
		Windows:      map[int64]map[string]WindowState{},
	}
	// Save 始终按当前版本写 envelope，这里只降级 payload 内版本号。
	legacy.Version = 1
	if err := store.Save(legacy); err != nil {
		t.Fatalf("save legacy: %v", err)
	}
	fresh := NewWithConfig(NewSliceSource(recoveryV2Events()), cfgV2(), NewSink(16), NewStore(dir))
	err := fresh.Restore()
	if !errors.Is(err, ErrCheckpointVersion) {
		t.Fatalf("legacy v1 must be rejected with ErrCheckpointVersion, got %v", err)
	}
	t.Logf("legacy v1 checkpoint rejected: %v", err)
}
