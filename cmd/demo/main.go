// demo 演示本地流处理运行时：重复投递幂等、乱序/迟到、全新进程+空下游恢复与背压。
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

	// 可重放输入：上游至少一次投递，重放里夹着重复事件（d2/d4 重复出现，且顺序打乱）。
	events := []stream.Event{
		{ID: "d1", Key: "a", EventTime: 1, Value: 1},
		{ID: "d2", Key: "b", EventTime: 3, Value: 2},
		{ID: "d3", Key: "a", EventTime: 8, Value: 3},
		{ID: "d2", Key: "b", EventTime: 3, Value: 2},  // 同批重复
		{ID: "d4", Key: "a", EventTime: 12, Value: 4}, // 推进水位线关闭 [0,10)
		{ID: "d5", Key: "b", EventTime: 15, Value: 5},
		{ID: "d6", Key: "a", EventTime: 21, Value: 6}, // 关闭 [10,20)
		{ID: "d4", Key: "a", EventTime: 12, Value: 4}, // 与正常事件混排的重复
		{ID: "d7", Key: "c", EventTime: 22, Value: 7},
		{ID: "d8", Key: "a", EventTime: 35, Value: 8}, // 关闭 [20,30)
		{ID: "d9", Key: "a", EventTime: 2, Value: 99}, // 超范围迟到，将被记录并丢弃
	}

	store := stream.NewStore(dir)

	// 第一个进程：处理前 5 条后做检查点（结果已对外可见），再处理几条后“被换掉”。
	src1 := stream.NewSliceSource(events)
	sink1 := stream.NewSink(4)
	rt1 := stream.New(src1, 10, 5, 1<<62, sink1, store)
	for i := 0; i < 5; i++ {
		if _, err := rt1.Step(ctx); err != nil {
			log.Fatal(err)
		}
	}
	if err := rt1.Checkpoint(); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("checkpoint: offset=%d nextSeq=%d watermark=%d (visible to old downstream)\n",
		src1.Offset(), rt1.NextSeq(), rt1.Watermark())
	fmt.Println("--- process replaced: new process, brand-new EMPTY downstream ---")

	// 全新进程：全新的可重放源 + 全新的空 Sink，手里只有 dir 里的落盘状态与可重放输入。
	src2 := stream.NewSliceSource(events)
	sink2 := stream.NewSink(2) // 极小缓冲：恢复重放历史时空下游也会背压，内存有界
	rt2 := stream.New(src2, 10, 5, 1<<62, sink2, store)

	// 模拟一个“全新且缓慢消费”的下游：恢复重放历史结果时由有界 Sink 自然背压。
	stopDrain := make(chan struct{})
	drainDone := make(chan struct{})
	go func() {
		defer close(drainDone)
		for {
			select {
			case <-stopDrain:
				return
			default:
			}
			if p := sink2.Pending(); p > 0 {
				sink2.ConfirmUpTo(sink2.ConfirmedSeq() + int64(p))
			}
			time.Sleep(time.Millisecond)
		}
	}()
	if err := rt2.Restore(ctx); err != nil {
		log.Fatal(err)
	}
	close(stopDrain)
	<-drainDone
	fmt.Printf("restored&replayed: offset=%d nextSeq=%d watermark=%d\n",
		src2.Offset(), rt2.NextSeq(), rt2.Watermark())

	fmt.Println("--- results re-visible to the empty downstream (deterministic order) ---")
	for _, r := range sink2.Confirmed() {
		fmt.Printf("result seq=%d window=[%d,%d) key=%s count=%d sum=%d\n",
			r.Seq, r.WindowStart, r.WindowEnd, r.Key, r.Count, r.Sum)
	}
	fmt.Printf("duplicates=%d (counted separately, never change aggregates)\n", len(rt2.Duplicates))
	for _, d := range rt2.Duplicates {
		fmt.Printf("  duplicate id=%s key=%s eventTime=%d\n", d.Event.ID, d.Event.Key, d.Event.EventTime)
	}
	for _, ld := range rt2.LateDropped {
		fmt.Printf("late-dropped eventTime=%d window=[%d,%d) watermark=%d reason=%s\n",
			ld.Event.EventTime, ld.WindowStart, ld.WindowEnd, ld.Watermark, ld.Reason)
	}
	for _, e := range rt2.DataErrors {
		fmt.Printf("data-error: %v\n", e)
	}
}
