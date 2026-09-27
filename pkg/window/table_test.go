package window

import (
	"testing"
	"time"

	"github.com/highcumontoa/realtime-stream-runtime-go/pkg/model"
)

var base = time.Unix(1_000, 0).UTC()

func at(sec int) time.Time { return base.Add(time.Duration(sec) * time.Second) }

func ev(seq int64, key string, v float64, sec int) model.Event {
	return model.Event{SeqID: seq, Key: key, Value: v, EventAt: at(sec)}
}

func newTable(t *testing.T, lateness time.Duration) *Table {
	t.Helper()
	a, err := NewTumblingAssigner(10 * time.Second)
	if err != nil {
		t.Fatalf("assigner: %v", err)
	}
	return NewTable(a, lateness)
}

func TestNormalWindowFiring(t *testing.T) {
	tab := newTable(t, 5*time.Second)

	if oc, _ := tab.ApplyEvent(ev(1, "a", 10, 1)); oc != OutcomeAccepted {
		t.Fatalf("event 1: got %v", oc)
	}
	if oc, _ := tab.ApplyEvent(ev(2, "a", 5, 2)); oc != OutcomeAccepted {
		t.Fatalf("event 2: got %v", oc)
	}
	if oc, _ := tab.ApplyEvent(ev(3, "b", 7, 3)); oc != OutcomeAccepted {
		t.Fatalf("event 3: got %v", oc)
	}

	t.Logf("advance watermark to %s (window [0,10) closes)", at(10).Format("15:04:05"))
	fired, purged := tab.AdvanceWatermark(at(10))
	if len(purged) != 0 {
		t.Fatalf("unexpected purges: %v", purged)
	}
	if len(fired) != 2 {
		t.Fatalf("expected 2 fired outputs, got %d", len(fired))
	}
	// 确定性顺序：窗口起点升序、key 升序。
	if fired[0].Key != "a" || fired[0].Sum != 15 || fired[0].Count != 2 || fired[0].Kind != model.KindFired || fired[0].Version != 1 {
		t.Fatalf("fired[0] = %+v", fired[0])
	}
	if fired[1].Key != "b" || fired[1].Sum != 7 || fired[1].Version != 1 {
		t.Fatalf("fired[1] = %+v", fired[1])
	}
}

func TestLateWithinAllowedProducesCorrection(t *testing.T) {
	tab := newTable(t, 5*time.Second)
	tab.ApplyEvent(ev(1, "a", 10, 1))
	tab.AdvanceWatermark(at(10)) // 窗口 [0,10) FIRED，保留到 wm=15

	t.Logf("late event seq=2 eventAt=%s arrives at wm=%s (within 5s lateness)",
		at(2).Format("15:04:05"), at(10).Format("15:04:05"))
	oc, corr := tab.ApplyEvent(ev(2, "a", 3, 2))
	if oc != OutcomeAcceptedLate {
		t.Fatalf("expected OutcomeAcceptedLate, got %v", oc)
	}
	if corr == nil || corr.Kind != model.KindCorrection || corr.Sum != 13 || corr.Version != 2 {
		t.Fatalf("correction = %+v", corr)
	}

	t.Logf("advance watermark to %s: window end+lateness=15 purged", at(15).Format("15:04:05"))
	_, purged := tab.AdvanceWatermark(at(15))
	if len(purged) != 1 || !purged[0].Equal(at(0)) {
		t.Fatalf("expected purge of window 0, got %v", purged)
	}
}

func TestOutOfRangeLateDistinctFromDataError(t *testing.T) {
	tab := newTable(t, 5*time.Second)
	tab.ApplyEvent(ev(1, "a", 10, 1))
	tab.AdvanceWatermark(at(15)) // 窗口 [0,10) 已 FIRED 且被清除

	// 合法数据，但窗口已清除 → 超范围迟到（不是数据错误）。
	t.Logf("out-of-range late event seq=2 eventAt=%s at wm=%s",
		at(2).Format("15:04:05"), at(15).Format("15:04:05"))
	oc, _ := tab.ApplyEvent(ev(2, "a", 3, 2))
	if oc != OutcomeOutOfRangeLate {
		t.Fatalf("expected OutcomeOutOfRangeLate, got %v", oc)
	}

	// 数据本身不合法 → 数据错误。
	bad := ev(3, "", 1, 20)
	oc, _ = tab.ApplyEvent(bad)
	if oc != OutcomeDataError {
		t.Fatalf("expected OutcomeDataError, got %v", oc)
	}
	if err := bad.Validate(); err == nil {
		t.Fatal("expected validation error for empty key")
	}
}

func TestDuplicateSeqID(t *testing.T) {
	tab := newTable(t, 5*time.Second)
	if oc, _ := tab.ApplyEvent(ev(1, "a", 10, 1)); oc != OutcomeAccepted {
		t.Fatalf("first: %v", oc)
	}
	if oc, _ := tab.ApplyEvent(ev(1, "a", 10, 1)); oc != OutcomeDuplicate {
		t.Fatalf("duplicate: %v", oc)
	}
	// 重复不得改变聚合结果。
	fired, _ := tab.AdvanceWatermark(at(10))
	if len(fired) != 1 || fired[0].Sum != 10 || fired[0].Count != 1 {
		t.Fatalf("duplicate leaked into aggregation: %+v", fired)
	}
}

func TestWatermarkIsMonotonic(t *testing.T) {
	tab := newTable(t, 5*time.Second)
	tab.ApplyEvent(ev(1, "a", 10, 1))
	tab.AdvanceWatermark(at(10))
	// 水位线回退/原地推进都是空操作。
	fired, purged := tab.AdvanceWatermark(at(5))
	if len(fired) != 0 || len(purged) != 0 {
		t.Fatalf("regressed watermark had effect: fired=%v purged=%v", fired, purged)
	}
	if !tab.Watermark().Equal(at(10)) {
		t.Fatalf("watermark moved backwards: %s", tab.Watermark())
	}
}

func TestSnapshotRestoreRoundTrip(t *testing.T) {
	tab := newTable(t, 5*time.Second)
	tab.ApplyEvent(ev(1, "a", 10, 1))
	tab.ApplyEvent(ev(2, "a", 5, 2))
	tab.AdvanceWatermark(at(10)) // FIRED，窗口保留中

	snap := tab.Snapshot(2)
	t.Logf("snapshot at position=%d watermark=%s", snap.Position, snap.Watermark.Format("15:04:05"))

	restored := newTable(t, 5*time.Second)
	if err := restored.Restore(snap); err != nil {
		t.Fatalf("restore: %v", err)
	}
	// 恢复后：重复事件仍被识别。
	if oc, _ := restored.ApplyEvent(ev(1, "a", 10, 1)); oc != OutcomeDuplicate {
		t.Fatalf("duplicate after restore: %v", oc)
	}
	// 恢复后：允许范围内的迟到仍产生版本连续的 CORRECTION。
	oc, corr := restored.ApplyEvent(ev(3, "a", 3, 2))
	if oc != OutcomeAcceptedLate || corr == nil || corr.Sum != 18 || corr.Version != 2 {
		t.Fatalf("correction after restore: oc=%v corr=%+v", oc, corr)
	}
}
