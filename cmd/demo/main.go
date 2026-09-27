// demo 演示本地流处理运行时：乱序事件、迟到丢弃、检查点恢复与背压。
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

	events := []stream.Event{
		{Key: "a", EventTime: 1, Value: 1},
		{Key: "b", EventTime: 3, Value: 2},
		{Key: "a", EventTime: 8, Value: 3},
		{Key: "a", EventTime: 12, Value: 4},
		{Key: "b", EventTime: 15, Value: 5},
		{Key: "a", EventTime: 21, Value: 6},
		{Key: "c", EventTime: 22, Value: 7},
		{Key: "a", EventTime: 35, Value: 8},
		{Key: "a", EventTime: 2, Value: 99}, // 超范围迟到，将被记录并丢弃
	}

	store := stream.NewStore(dir)
	sink := stream.NewSink(4)
	src := stream.NewSliceSource(events)
	rt := stream.New(src, 10, 5, sink, store)

	// 处理一半后做检查点，再模拟崩溃。
	for i := 0; i < 5; i++ {
		if _, err := rt.Step(ctx); err != nil {
			log.Fatal(err)
		}
	}
	if err := rt.Checkpoint(); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("checkpoint: offset=%d nextSeq=%d watermark=%d\n", src.Offset(), rt.NextSeq(), rt.Watermark())
	fmt.Println("--- crash ---")

	// 重启恢复。
	rt2 := stream.New(src, 10, 5, sink, store)
	if err := rt2.Restore(); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("restored: offset=%d nextSeq=%d watermark=%d\n", src.Offset(), rt2.NextSeq(), rt2.Watermark())

	// 模拟下游缓慢消费（背压由有界 Sink 自然形成）。
	go func() {
		for {
			time.Sleep(20 * time.Millisecond)
			sink.ConfirmUpTo(sink.ConfirmedSeq() + int64(sink.Pending()))
		}
	}()
	if err := rt2.Run(ctx); err != nil {
		log.Fatal(err)
	}

	for _, r := range sink.Confirmed() {
		fmt.Printf("result seq=%d window=[%d,%d) key=%s count=%d sum=%d\n",
			r.Seq, r.WindowStart, r.WindowEnd, r.Key, r.Count, r.Sum)
	}
	for _, ld := range rt2.LateDropped {
		fmt.Printf("late-dropped eventTime=%d window=[%d,%d) watermark=%d reason=%s\n",
			ld.Event.EventTime, ld.WindowStart, ld.WindowEnd, ld.Watermark, ld.Reason)
	}
}
