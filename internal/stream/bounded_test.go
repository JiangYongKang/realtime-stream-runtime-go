package stream

import (
	"context"
	"strconv"
	"testing"
)

// buildBoundedEvents 构造长时间输入：每个窗口一条唯一事件，时间间隔 10ms；
// 每 10 个窗口额外重投 2 个窗口前的事件（时间差 20ms < 保留范围 30ms）。
func buildBoundedEvents(nWindows int64) []Event {
	var evs []Event
	var dupCount int
	for i := int64(0); i < nWindows; i++ {
		evs = append(evs, Event{ID: idFor(i), Key: "k", EventTime: i*10 + 1, Value: 1})
		if i >= 2 && i%10 == 0 {
			evs = append(evs, Event{ID: idFor(i - 2), Key: "k", EventTime: (i-2)*10 + 1, Value: 1})
			dupCount++
		}
	}
	return evs
}

// 长时间运行：去重相关状态必须随保留范围回收而有界，不能随运行时间无限增长；
// 落在保留范围内的重复照样去重；检查点保存/恢复的去重状态同样有界且有效。
func TestDedupStateBoundedByRetention(t *testing.T) {
	ctx := context.Background()
	const nWindows = 200
	evs := buildBoundedEvents(nWindows)
	wantDups := 0
	for i := int64(2); i < nWindows; i++ {
		if i%10 == 0 {
			wantDups++
		}
	}

	dir := t.TempDir()
	store := NewStore(dir)
	src := NewSliceSource(evs)
	sink := NewSink(1024)
	rt := NewWithConfig(src, Config{
		WindowSize: 10, AllowedLateness: 5, DedupRetention: 30, Generation: "g1",
	}, sink, store)

	peak := 0
	for {
		done, err := rt.Step(ctx)
		if err != nil {
			t.Fatalf("step: %v", err)
		}
		if done {
			break
		}
		if rt.DedupSeen() > peak {
			peak = rt.DedupSeen()
		}
	}
	if err := rt.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	t.Logf("after %d windows: dedupSeen=%d peak=%d evictions=%d duplicates=%d watermark=%d",
		nWindows, rt.DedupSeen(), peak, rt.DedupEvictions, len(rt.Duplicates), rt.Watermark())

	// 有界：200 个窗口喂下去，已见集合必须远小于总事件量（约保留 3~4 个窗口）。
	if rt.DedupSeen() >= nWindows {
		t.Fatalf("dedup state unbounded: seen=%d total=%d", rt.DedupSeen(), nWindows)
	}
	if peak > 10 {
		t.Fatalf("dedup state peak too large for retention=30ms window=10ms: %d", peak)
	}
	if rt.DedupEvictions == 0 {
		t.Fatalf("expected evictions to happen under long run")
	}
	// 保留范围内的近期重复全部被去重。
	if len(rt.Duplicates) != wantDups {
		t.Fatalf("retention-window duplicates not suppressed: got %d want %d",
			len(rt.Duplicates), wantDups)
	}

	// 检查点落盘的去重状态同样有界。
	cp, err := store.Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(cp.Seen) != rt.DedupSeen() {
		t.Fatalf("checkpoint seen size %d != runtime seen %d", len(cp.Seen), rt.DedupSeen())
	}
	t.Logf("checkpoint carries bounded dedup state: sourceOffset=%d seen=%d",
		cp.SourceOffset, len(cp.Seen))

	// 全新进程恢复（空下游 -> 全量重放）：重放结束后保留范围内的旧 ID 仍记得，
	// 末尾追加一条"近期重复"必须仍被去重。
	replayEvs := append(buildBoundedEvents(nWindows),
		Event{ID: idFor(nWindows - 1), Key: "k", EventTime: (nWindows-1)*10 + 1, Value: 1})
	replaySrc := NewSliceSource(replayEvs)
	freshSink := NewSink(1024)
	rt2 := NewWithConfig(replaySrc, Config{
		WindowSize: 10, AllowedLateness: 5, DedupRetention: 30, Generation: "g1",
	}, freshSink, NewStore(dir))
	if err := rt2.Restore(); err != nil {
		t.Fatalf("restore: %v", err)
	}
	t.Logf("fresh process restored by replay with bounded dedup state: sourceOffset=0 nextSeq=0")
	if err := rt2.Run(ctx); err != nil {
		t.Fatalf("resumed run: %v", err)
	}
	if got := len(rt2.Duplicates); got != wantDups+1 {
		t.Fatalf("in-retention duplicate must still dedup after restore: got %d want %d",
			got, wantDups+1)
	}
	t.Logf("in-retention duplicate still suppressed after restore: total duplicates=%d watermark=%d",
		len(rt2.Duplicates), rt2.Watermark())
}

// 永不回收（retention=0）时状态随输入增长是显式选择；有界保留时必须被回收。
func TestRetentionZeroKeepsEverything(t *testing.T) {
	var evs []Event
	for i := int64(0); i < 20; i++ {
		evs = append(evs, Event{ID: idFor(i), Key: "k", EventTime: i * 10, Value: 1})
	}
	sink := NewSink(64)
	rt := NewWithConfig(NewSliceSource(evs), Config{
		WindowSize: 10, AllowedLateness: 5, DedupRetention: 0, Generation: "g1",
	}, sink, NewStore(t.TempDir()))
	if err := rt.Run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if rt.DedupSeen() != 20 || rt.DedupEvictions != 0 {
		t.Fatalf("retention=0 must keep all ids: seen=%d evictions=%d",
			rt.DedupSeen(), rt.DedupEvictions)
	}
	t.Logf("retention=0 keeps all %d ids (explicit unbounded choice)", rt.DedupSeen())
}

func idFor(i int64) string {
	return "id-" + strconv.FormatInt(i, 10)
}
