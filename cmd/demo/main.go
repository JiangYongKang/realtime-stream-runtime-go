// demo 演示本地流处理运行时：乱序事件、重复幂等、迟到丢弃、跨进程恢复与背压。
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/highcumontoa/realtime-stream-runtime-go/internal/stream"
)

func main() {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "stream-demo")
	if err != nil {
		log.Fatal(err)
	}

	// 上游至少一次投递：e3 重复一次、e1 乱序后再重复一次。
	events := []stream.Event{
		{ID: "e1", Key: "a", EventTime: 1, Value: 1},
		{ID: "e2", Key: "b", EventTime: 3, Value: 2},
		{ID: "e3", Key: "a", EventTime: 8, Value: 3},
		{ID: "e3", Key: "a", EventTime: 8, Value: 3}, // 重复投递
		{ID: "e4", Key: "a", EventTime: 12, Value: 4},
		{ID: "e5", Key: "b", EventTime: 15, Value: 5},
		{ID: "e6", Key: "a", EventTime: 21, Value: 6},
		{ID: "e7", Key: "c", EventTime: 22, Value: 7},
		{ID: "e8", Key: "a", EventTime: 35, Value: 8},
		{ID: "e1", Key: "a", EventTime: 1, Value: 1},  // 乱序重复
		{ID: "e9", Key: "a", EventTime: 2, Value: 99}, // 超范围迟到
	}

	cfg := stream.Config{
		WindowSize:      10,
		AllowedLateness: 5,
		DedupRetention:  60, // 去重状态按事件时间保留 60ms，超出自动回收
		Generation:      "demo-lineage-1",
	}
	store := stream.NewStore(dir)
	sink := stream.NewSink(4)
	src := stream.NewSliceSource(events)
	rt := stream.NewWithConfig(src, cfg, sink, store)

	// 处理一半后做检查点，再模拟进程被换掉（只留下落盘状态与可重放输入）。
	for i := 0; i < 5; i++ {
		if _, err := rt.Step(ctx); err != nil {
			log.Fatal(err)
		}
	}
	if err := rt.Checkpoint(); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("checkpoint: offset=%d nextSeq=%d watermark=%d\n", src.Offset(), rt.NextSeq(), rt.Watermark())
	fmt.Println("--- process replaced: only checkpoint + replayable input survive ---")

	// 全新进程 + 全新 Source + 全新空下游，只复用落盘 Store。
	newSrc := stream.NewSliceSource(events)
	emptySink := stream.NewSink(4)
	rt2 := stream.NewWithConfig(newSrc, cfg, emptySink, stream.NewStore(dir))
	if err := rt2.Restore(); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("restored onto empty downstream via full replay: offset=%d nextSeq=%d watermark=%d\n",
		newSrc.Offset(), rt2.NextSeq(), rt2.Watermark())

	// 模拟下游缓慢消费（背压由有界 Sink 自然形成）。
	go func() {
		for {
			time.Sleep(20 * time.Millisecond)
			if p := emptySink.Pending(); p > 0 {
				emptySink.ConfirmUpTo(emptySink.ConfirmedSeq() + int64(p))
			}
		}
	}()
	if err := rt2.Run(ctx); err != nil {
		log.Fatal(err)
	}

	for _, r := range emptySink.Confirmed() {
		fmt.Printf("result seq=%d window=[%d,%d) key=%s count=%d sum=%d\n",
			r.Seq, r.WindowStart, r.WindowEnd, r.Key, r.Count, r.Sum)
	}
	for _, d := range rt2.Duplicates {
		fmt.Printf("duplicate-suppressed id=%s key=%s eventTime=%d watermark=%d\n",
			d.Event.ID, d.Event.Key, d.Event.EventTime, d.Watermark)
	}
	for _, ld := range rt2.LateDropped {
		fmt.Printf("late-dropped id=%s eventTime=%d window=[%d,%d) watermark=%d reason=%s\n",
			ld.Event.ID, ld.Event.EventTime, ld.WindowStart, ld.WindowEnd, ld.Watermark, ld.Reason)
	}
	for _, e := range rt2.DataErrors {
		fmt.Printf("data-error: %v\n", e)
	}
	fmt.Printf("dedup state bounded: seenNow=%d evictions=%d\n", rt2.DedupSeen(), rt2.DedupEvictions)
}
