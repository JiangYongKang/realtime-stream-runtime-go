package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/highcumontoa/realtime-stream-runtime-go/pkg/checkpoint"
	"github.com/highcumontoa/realtime-stream-runtime-go/pkg/engine"
	"github.com/highcumontoa/realtime-stream-runtime-go/pkg/model"
)

// demo 用一条确定性的乱序流演示：
//   - 事件时间窗口聚合（10s 滚动窗口）；
//   - 允许迟到 5s 内的迟到事件 → FIRED 后产生 CORRECTION；
//   - 超出范围的迟到事件 → OUT_OF_RANGE_LATE（与 DATA_ERROR 分开记录）；
//   - 重复 SeqID → DUPLICATE；
//   - 第 1 次检查点后模拟崩溃，用同一份可重放流重启，输出不重不丢。
func main() {
	base := time.Unix(1_000, 0).UTC()
	at := func(sec int) time.Time { return base.Add(time.Duration(sec) * time.Second) }

	// 乱序到达：事件时间在流中来回跳动；并包含重复(seq=2)、超范围迟到(seq=7)、
	// 数据错误(seq=8, 空键)。
	stream := []model.StreamItem{
		engine.EventItem(1, "a", 10, at(1)),
		engine.EventItem(2, "a", 5, at(2)),
		engine.WatermarkItem(at(8)),
		engine.EventItem(3, "b", 7, at(3)),
		engine.WatermarkItem(at(12)),       // [00:00,00:10) 窗口 FIRED
		engine.EventItem(4, "a", 3, at(4)), // 在允许迟到内 → a 的 CORRECTION
		engine.EventItem(2, "a", 5, at(2)), // 重复
		engine.WatermarkItem(at(16)),       // wm=16 > end10+5=15 → 窗口被清除
		engine.EventItem(7, "a", 9, at(9)), // 超范围迟到 → OUT_OF_RANGE_LATE
		engine.EventItem(8, "", 1, at(11)), // 数据错误 → DATA_ERROR
		engine.WatermarkItem(at(22)),
	}

	dir, err := os.MkdirTemp("", "rsr-demo-ckpt-")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)

	cfg := engine.Config{
		WindowSize:      10 * time.Second,
		AllowedLateness: 5 * time.Second,
		SinkCapacity:    2,
		CheckpointEvery: 4,
	}

	runOnce := func(label string, crash bool) (*engine.Result, error) {
		src := engine.NewStaticSource(stream)
		sink := engine.NewSink(cfg.SinkCapacity)
		sink.SetHandler(func(o model.WindowOutput) error {
			fmt.Printf("  >> sink %s window=[%s,%s) key=%s sum=%.0f count=%d v%d\n",
				o.Kind, o.WindowStart.Format("15:04:05"), o.WindowEnd.Format("15:04:05"),
				o.Key, o.Sum, o.Count, o.Version)
			return nil
		})
		sink.SetExclusionHandler(func(x model.Exclusion) error {
			fmt.Printf("  !! excluded seq=%d reason=%s eventAt=%s detail=%q\n",
				x.SeqID, x.Reason, x.EventAt.Format("15:04:05"), x.Detail)
			return nil
		})
		store, err := checkpoint.NewDirStore(filepath.Join(dir, "ckpt"))
		if err != nil {
			return nil, err
		}
		eng, err := engine.New(cfg, src, sink, store)
		if err != nil {
			return nil, err
		}
		if crash {
			eng.SetCrashHook(func(n int, st *checkpoint.State) bool {
				fmt.Printf("[crash-hook] checkpoint #%d committed at position=%d wm=%s; simulating crash\n",
					n, st.Position, st.Watermark.Format(time.RFC3339))
				return n == 1
			})
		}
		fmt.Printf("==== %s ====\n", label)
		res, err := eng.Run()
		if err != nil {
			return res, err
		}
		fmt.Printf("[%s] done position=%d watermark=%s checkpoints=%d outputs=%d dup=%d late=%d dataErr=%d\n",
			label, res.FinalPosition, res.FinalWatermark.Format(time.RFC3339),
			res.Metrics.Checkpoints, res.Metrics.OutputsDelivered,
			res.Metrics.Duplicates, res.Metrics.ExclusionsLate, res.Metrics.ExclusionsData)
		return res, nil
	}

	if _, err := runOnce("run-1 (crash after checkpoint #1)", true); err != nil {
		if !errors.Is(err, engine.ErrSimulatedCrash) {
			fmt.Fprintf(os.Stderr, "run-1 failed: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("run-1 terminated by simulated crash as designed")
	}
	if _, err := runOnce("run-2 (replay from last checkpoint)", false); err != nil {
		fmt.Fprintf(os.Stderr, "run-2 failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("==== demo finished: outputs across two runs are exactly-once ====")
}
