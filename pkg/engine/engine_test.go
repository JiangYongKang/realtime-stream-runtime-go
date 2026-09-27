package engine

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/highcumontoa/realtime-stream-runtime-go/pkg/checkpoint"
	"github.com/highcumontoa/realtime-stream-runtime-go/pkg/model"
)

var base = time.Unix(1_000, 0).UTC()

func at(sec int) time.Time { return base.Add(time.Duration(sec) * time.Second) }

// demoStream 覆盖：正常窗口、允许范围内迟到（CORRECTION）、超范围迟到、数据错误。
func demoStream() []model.StreamItem {
	return []model.StreamItem{
		EventItem(1, "a", 10, at(1)),
		EventItem(2, "a", 5, at(2)),
		WatermarkItem(at(8)),
		EventItem(3, "b", 7, at(3)),
		WatermarkItem(at(10)),       // 窗口 [0,10) FIRED
		EventItem(4, "a", 3, at(4)), // 允许范围内迟到 → CORRECTION
		WatermarkItem(at(15)),       // 窗口 [0,10) 被清除
		EventItem(5, "a", 9, at(5)), // 超范围迟到
		EventItem(6, "", 1, at(6)),  // 数据错误（空键）
		WatermarkItem(at(20)),
	}
}

func testCfg() Config {
	return Config{
		WindowSize:      10 * time.Second,
		AllowedLateness: 5 * time.Second,
		SinkCapacity:    4,
		CheckpointEvery: 3,
	}
}

type collector struct {
	outputs []model.WindowOutput
	excls   []model.Exclusion
}

func (c *collector) sink(capacity int) *Sink {
	s := NewSink(capacity)
	s.SetHandler(func(o model.WindowOutput) error {
		c.outputs = append(c.outputs, o)
		return nil
	})
	s.SetExclusionHandler(func(x model.Exclusion) error {
		c.excls = append(c.excls, x)
		return nil
	})
	return s
}

func outputsKey(o model.WindowOutput) string {
	return fmt.Sprintf("%s|%s|%s|v%d|sum=%.1f|cnt=%d",
		o.Kind, o.WindowStart.Format("15:04:05"), o.Key, o.Version, o.Sum, o.Count)
}

func TestRecoveryExactlyOnce(t *testing.T) {
	dir := t.TempDir()
	cfg := testCfg()

	// 基线：无中断完整运行一次。
	var baseline collector
	{
		src := NewStaticSource(demoStream())
		sink := baseline.sink(cfg.SinkCapacity)
		store, err := checkpoint.NewDirStore(dir + "/baseline")
		if err != nil {
			t.Fatal(err)
		}
		eng, err := New(cfg, src, sink, store)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := eng.Run(); err != nil {
			t.Fatalf("baseline run: %v", err)
		}
		sink.Close()
	}

	// 中断恢复：第 1 个检查点后崩溃，重启续跑。
	var combined collector
	{
		src := NewStaticSource(demoStream())
		sink := combined.sink(cfg.SinkCapacity)
		store, err := checkpoint.NewDirStore(dir + "/crash")
		if err != nil {
			t.Fatal(err)
		}
		eng, err := New(cfg, src, sink, store)
		if err != nil {
			t.Fatal(err)
		}
		eng.SetCrashHook(func(n int, st *checkpoint.State) bool {
			t.Logf("run-1: checkpoint #%d committed at position=%d watermark=%s; crashing",
				n, st.Position, st.Watermark.Format("15:04:05"))
			return n == 1
		})
		_, err = eng.Run()
		if !errors.Is(err, ErrSimulatedCrash) {
			t.Fatalf("run-1 expected simulated crash, got %v", err)
		}
		sink.Close()
	}
	{
		src := NewStaticSource(demoStream())
		sink := combined.sink(cfg.SinkCapacity)
		store, err := checkpoint.NewDirStore(dir + "/crash")
		if err != nil {
			t.Fatal(err)
		}
		pos, _ := store.LastPosition()
		t.Logf("run-2: recovering from position=%d", pos)
		eng, err := New(cfg, src, sink, store)
		if err != nil {
			t.Fatal(err)
		}
		res, err := eng.Run()
		if err != nil {
			t.Fatalf("run-2: %v", err)
		}
		t.Logf("run-2: done position=%d watermark=%s", res.FinalPosition, res.FinalWatermark.Format("15:04:05"))
		sink.Close()
	}

	// 断言 1：输出序列与无中断基线完全一致（不重、不丢、不乱序）。
	if len(combined.outputs) != len(baseline.outputs) {
		t.Fatalf("output count mismatch: combined=%d baseline=%d\ncombined=%v",
			len(combined.outputs), len(baseline.outputs), combined.outputs)
	}
	for i := range baseline.outputs {
		if outputsKey(combined.outputs[i]) != outputsKey(baseline.outputs[i]) {
			t.Fatalf("output %d mismatch:\n combined=%s\n baseline=%s",
				i, outputsKey(combined.outputs[i]), outputsKey(baseline.outputs[i]))
		}
	}
	// 断言 2：排除记录一致（超范围迟到与数据错误可区分）。
	if len(combined.excls) != len(baseline.excls) {
		t.Fatalf("exclusion count mismatch: %d vs %d", len(combined.excls), len(baseline.excls))
	}
	var lateReasons, dataReasons int
	for _, x := range combined.excls {
		switch x.Reason {
		case model.ReasonOutOfRangeLate:
			lateReasons++
		case model.ReasonDataError:
			dataReasons++
		}
	}
	if lateReasons != 1 || dataReasons != 1 {
		t.Fatalf("expected 1 out-of-range late + 1 data error, got %d/%d", lateReasons, dataReasons)
	}
}

func TestCorruptedCheckpointRefusesStartup(t *testing.T) {
	dir := t.TempDir()
	cfg := testCfg()

	// 先正常跑完，留下检查点。
	{
		src := NewStaticSource(demoStream())
		sink := NewSink(cfg.SinkCapacity)
		store, err := checkpoint.NewDirStore(dir)
		if err != nil {
			t.Fatal(err)
		}
		eng, err := New(cfg, src, sink, store)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := eng.Run(); err != nil {
			t.Fatalf("initial run: %v", err)
		}
		sink.Close()
	}

	// 注入字节损坏 → 重启必须被拒绝，且原因可区分。
	store, err := checkpoint.NewDirStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Corrupt("bytes"); err != nil {
		t.Fatal(err)
	}
	eng, err := New(cfg, NewStaticSource(demoStream()), NewSink(cfg.SinkCapacity), store)
	if err != nil {
		t.Fatal(err)
	}
	_, err = eng.Run()
	if !errors.Is(err, checkpoint.ErrChecksumMismatch) {
		t.Fatalf("expected ErrChecksumMismatch, got %v", err)
	}
	t.Logf("corrupted checkpoint refused at startup: %v", err)
}

func TestIncompatibleVersionRefusesStartup(t *testing.T) {
	dir := t.TempDir()
	cfg := testCfg()
	{
		src := NewStaticSource(demoStream())
		sink := NewSink(cfg.SinkCapacity)
		store, err := checkpoint.NewDirStore(dir)
		if err != nil {
			t.Fatal(err)
		}
		eng, err := New(cfg, src, sink, store)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := eng.Run(); err != nil {
			t.Fatalf("initial run: %v", err)
		}
		sink.Close()
	}
	store, err := checkpoint.NewDirStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Corrupt("version"); err != nil {
		t.Fatal(err)
	}
	eng, err := New(cfg, NewStaticSource(demoStream()), NewSink(cfg.SinkCapacity), store)
	if err != nil {
		t.Fatal(err)
	}
	_, err = eng.Run()
	if !errors.Is(err, checkpoint.ErrVersionIncompatible) {
		t.Fatalf("expected ErrVersionIncompatible, got %v", err)
	}
	t.Logf("incompatible version refused at startup: %v", err)
}

func TestBackpressureThrottlesAndPreservesOrder(t *testing.T) {
	dir := t.TempDir()
	cfg := testCfg()
	cfg.SinkCapacity = 1 // 最小容量：持续过载
	cfg.CheckpointEvery = 0

	// 3 个窗口 × 2 个 key，共 6 条 FIRED 输出。
	var stream []model.StreamItem
	seq := int64(1)
	for w := 0; w < 3; w++ {
		stream = append(stream,
			EventItem(seq, "a", 1, at(w*10+1)), // 乱序：先发 b 再发 a
			EventItem(seq+1, "b", 2, at(w*10+2)),
		)
		seq += 2
	}
	stream = append(stream, WatermarkItem(at(35)))

	var got []model.WindowOutput
	sink := NewSink(cfg.SinkCapacity)
	sink.SetHandler(func(o model.WindowOutput) error {
		time.Sleep(3 * time.Millisecond) // 模拟下游处理能力不足
		got = append(got, o)
		return nil
	})
	store, err := checkpoint.NewDirStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	eng, err := New(cfg, NewStaticSource(stream), sink, store)
	if err != nil {
		t.Fatal(err)
	}
	res, err := eng.Run()
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	sink.Close()

	if res.Metrics.BackpressureBlocks == 0 {
		t.Fatal("expected backpressure blocks with capacity=1 and slow consumer")
	}
	t.Logf("backpressure: blocks=%d wait=%dms outputs=%d",
		res.Metrics.BackpressureBlocks, res.Metrics.BackpressureWaitNanos/1e6, len(got))

	if len(got) != 6 {
		t.Fatalf("expected 6 outputs, got %d", len(got))
	}
	// 确定性顺序：窗口起点升序、key 升序，且每个 (窗口,key) 恰好一次。
	seen := map[string]int{}
	for i, o := range got {
		k := fmt.Sprintf("%s|%s", o.WindowStart.Format("15:04:05"), o.Key)
		seen[k]++
		if i > 0 {
			prev := got[i-1]
			if prev.WindowStart.After(o.WindowStart) ||
				(prev.WindowStart.Equal(o.WindowStart) && prev.Key > o.Key) {
				t.Fatalf("out-of-order output at %d: %v then %v", i, prev, o)
			}
		}
	}
	for k, n := range seen {
		if n != 1 {
			t.Fatalf("duplicate delivery for %s: %d times", k, n)
		}
	}
}

func TestMultipleRestartsRemainExactlyOnce(t *testing.T) {
	dir := t.TempDir()
	cfg := testCfg() // CheckpointEvery=3，流共 10 个元素 → 3 个检查点
	var combined collector

	for attempt := 1; attempt <= 3; attempt++ {
		src := NewStaticSource(demoStream())
		sink := combined.sink(cfg.SinkCapacity)
		store, err := checkpoint.NewDirStore(dir)
		if err != nil {
			t.Fatal(err)
		}
		eng, err := New(cfg, src, sink, store)
		if err != nil {
			t.Fatal(err)
		}
		eng.SetCrashHook(func(n int, st *checkpoint.State) bool {
			t.Logf("attempt=%d checkpoint #%d position=%d watermark=%s",
				attempt, n, st.Position, st.Watermark.Format("15:04:05"))
			return n == attempt // 第 1 次在 cp1 后崩，第 2 次在 cp2 后崩，第 3 次不崩
		})
		res, err := eng.Run()
		sink.Close()
		if attempt < 3 {
			if !errors.Is(err, ErrSimulatedCrash) {
				t.Fatalf("attempt %d expected crash, got %v", attempt, err)
			}
			continue
		}
		if err != nil {
			t.Fatalf("attempt 3: %v", err)
		}
		t.Logf("attempt=3 complete at position=%d", res.FinalPosition)
	}

	// 三次运行累计投递的输出必须仍然恰好是 3 条：FIRED a、FIRED b、CORRECTION a。
	if len(combined.outputs) != 3 {
		t.Fatalf("expected exactly 3 outputs across restarts, got %d: %v",
			len(combined.outputs), combined.outputs)
	}
	kinds := map[string]int{}
	for _, o := range combined.outputs {
		kinds[fmt.Sprintf("%s@%s v%d", o.Key, o.WindowStart.Format("15:04:05"), o.Version)]++
	}
	if kinds["a@00:16:40 v1"] != 1 || kinds["b@00:16:40 v1"] != 1 || kinds["a@00:16:40 v2"] != 1 {
		t.Fatalf("unexpected output multiset: %v", kinds)
	}
}
