package engine

import (
	"errors"
	"time"

	"github.com/highcumontoa/realtime-stream-runtime-go/pkg/checkpoint"
	"github.com/highcumontoa/realtime-stream-runtime-go/pkg/model"
	"github.com/highcumontoa/realtime-stream-runtime-go/pkg/window"
)

// Config 是运行时配置。
type Config struct {
	WindowSize      time.Duration
	AllowedLateness time.Duration
	SinkCapacity    int
	// CheckpointEvery：每处理 N 个流元素做一次检查点；<=0 表示仅在流结束时检查。
	CheckpointEvery int
}

// Result 是一次 Run 的汇总结果。
type Result struct {
	FinalPosition  int64
	FinalWatermark time.Time
	Metrics        Metrics
}

// CrashHook 在某次检查点“状态已持久化、继续处理之前”触发，
// 返回 true 则模拟进程崩溃：Run 立即返回 ErrSimulatedCrash，
// 检查点之后的任何内存状态都不再可见（与真实崩溃一致）。
type CrashHook func(checkpointSeq int, state *checkpoint.State) bool

// ErrSimulatedCrash 表示崩溃钩子模拟的进程退出（测试用，不是恢复失败）。
var ErrSimulatedCrash = errors.New("simulated crash after committed checkpoint")

// Engine 是本地流处理运行时。
type Engine struct {
	cfg      Config
	source   ReplaySource
	sink     *Sink
	store    checkpoint.Store
	assigner window.Assigner
	hook     CrashHook
}

// New 创建运行时。
func New(cfg Config, source ReplaySource, sink *Sink, store checkpoint.Store) (*Engine, error) {
	a, err := window.NewTumblingAssigner(cfg.WindowSize)
	if err != nil {
		return nil, err
	}
	return &Engine{
		cfg:      cfg,
		source:   source,
		sink:     sink,
		store:    store,
		assigner: a,
	}, nil
}

// SetCrashHook 安装崩溃钩子（测试用）。
func (e *Engine) SetCrashHook(h CrashHook) { e.hook = h }

func toOutputRecord(o model.WindowOutput) checkpoint.OutputRecord {
	return checkpoint.OutputRecord{
		WindowStart: o.WindowStart, WindowEnd: o.WindowEnd, Key: o.Key,
		Sum: o.Sum, Count: o.Count, Kind: int(o.Kind), Version: o.Version,
	}
}

func fromOutputRecord(r checkpoint.OutputRecord) model.WindowOutput {
	return model.WindowOutput{
		WindowStart: r.WindowStart, WindowEnd: r.WindowEnd, Key: r.Key,
		Sum: r.Sum, Count: r.Count, Kind: model.OutputKind(r.Kind), Version: r.Version,
	}
}

func toExclusionRecord(x model.Exclusion) checkpoint.ExclusionRecord {
	return checkpoint.ExclusionRecord{
		SeqID: x.SeqID, Key: x.Key, EventAt: x.EventAt,
		Reason: int(x.Reason), Detail: x.Detail, ObservedAt: x.ObservedAt,
	}
}

// Run 执行流处理：
//  1. 尝试从最近检查点恢复；冷启动（ErrNoCheckpoint）从头开始；
//     损坏/版本不兼容/位点回退直接返回对应错误，绝不从中途静默继续；
//  2. 将源 SeekAfter 到已确认位点，只重放未确认部分；
//  3. 处理事件与水位线，窗口结果/排除经有界 sink 保序确认；
//  4. 周期性检查点：先 Drain sink（只把“已可见”结果计入），再原子提交。
func (e *Engine) Run() (*Result, error) {
	tab := window.NewTable(e.assigner, e.cfg.AllowedLateness)
	var m Metrics

	var startPos int64 = -1
	st, err := e.store.Load()
	switch {
	case err == nil:
		if err := tab.Restore(st); err != nil {
			return nil, err
		}
		startPos = st.Position
		outs := make([]model.WindowOutput, 0, len(st.Outputs))
		for _, r := range st.Outputs {
			outs = append(outs, fromOutputRecord(r))
		}
		exs := make([]model.Exclusion, 0, len(st.Exclusions))
		for _, r := range st.Exclusions {
			exs = append(exs, model.Exclusion{
				SeqID: r.SeqID, Key: r.Key, EventAt: r.EventAt,
				Reason: model.ExclusionReason(r.Reason), Detail: r.Detail, ObservedAt: r.ObservedAt,
			})
		}
		e.sink.Truncate(outs, exs)
	case errors.Is(err, checkpoint.ErrNoCheckpoint):
		// 冷启动
	default:
		return nil, err // ErrChecksumMismatch / ErrMalformed / ErrVersionIncompatible 等，拒绝启动
	}

	e.source.Reset()
	if startPos >= 0 {
		e.source.SeekAfter(startPos)
	}

	position := startPos // 无检查点时为 -1；首个元素后更新
	cpSeq := 0
	elemsSinceCheckpoint := 0

	// doCheckpoint 排空下游、采集已确认视图、提交检查点，并处理崩溃钩子。
	doCheckpoint := func() error {
		e.sink.Drain()
		cp := tab.Snapshot(position)
		view := e.sink.Confirmed()
		cp.Outputs = make([]checkpoint.OutputRecord, 0, len(view.Outputs))
		for _, o := range view.Outputs {
			cp.Outputs = append(cp.Outputs, toOutputRecord(o))
		}
		cp.Exclusions = make([]checkpoint.ExclusionRecord, 0, len(view.Exclusions))
		for _, x := range view.Exclusions {
			cp.Exclusions = append(cp.Exclusions, toExclusionRecord(x))
		}
		if err := e.store.Save(cp); err != nil {
			return err // 含 ErrPositionRegressed
		}
		cpSeq++
		m.Checkpoints++
		if e.hook != nil && e.hook(cpSeq, cp) {
			return ErrSimulatedCrash
		}
		return nil
	}

	// deliver 包装 sink 投递，统计背压阻塞次数与等待时长。
	deliver := func(o model.WindowOutput) error {
		before := e.sink.InFlight()
		t0 := time.Now()
		if err := e.sink.Deliver(o); err != nil {
			return err
		}
		if before >= e.cfg.SinkCapacity {
			m.BackpressureBlocks++
			m.BackpressureWaitNanos += time.Since(t0).Nanoseconds()
		}
		m.OutputsDelivered++
		return nil
	}

	for {
		item, ok := e.source.Next()
		if !ok {
			break
		}
		if item.IsWatermark() {
			wm := item.Watermark.UTC()
			if wm.After(tab.Watermark()) {
				fired, purged := tab.AdvanceWatermark(wm)
				m.WatermarksAdvanced++
				for _, o := range fired {
					if err := deliver(o); err != nil {
						return nil, err
					}
				}
				_ = purged // 窗口清除是确定性规则的一部分；迟到事件命中时会显式记录
			}
		} else {
			ev := *item.Event
			outcome, correction := tab.ApplyEvent(ev)
			switch outcome {
			case window.OutcomeAccepted:
				m.EventsProcessed++
			case window.OutcomeAcceptedLate:
				m.EventsProcessed++
				if err := deliver(*correction); err != nil {
					return nil, err
				}
			case window.OutcomeDuplicate:
				m.Duplicates++
				if err := e.sink.DeliverExclusion(model.Exclusion{
					SeqID: ev.SeqID, Key: ev.Key, EventAt: ev.EventAt,
					Reason:     model.ReasonDuplicate,
					Detail:     "seq id already processed; discarded deterministically",
					ObservedAt: time.Now().UTC(),
				}); err != nil {
					return nil, err
				}
			case window.OutcomeOutOfRangeLate:
				m.ExclusionsLate++
				if err := e.sink.DeliverExclusion(model.Exclusion{
					SeqID: ev.SeqID, Key: ev.Key, EventAt: ev.EventAt,
					Reason:     model.ReasonOutOfRangeLate,
					Detail:     "event later than watermark minus allowed lateness; window purged",
					ObservedAt: time.Now().UTC(),
				}); err != nil {
					return nil, err
				}
			case window.OutcomeDataError:
				m.ExclusionsData++
				if err := e.sink.DeliverExclusion(model.Exclusion{
					SeqID: ev.SeqID, Key: ev.Key, EventAt: ev.EventAt,
					Reason:     model.ReasonDataError,
					Detail:     ev.Validate().Error(),
					ObservedAt: time.Now().UTC(),
				}); err != nil {
					return nil, err
				}
			}
			position = ev.SeqID
		}

		elemsSinceCheckpoint++
		if e.cfg.CheckpointEvery > 0 && elemsSinceCheckpoint >= e.cfg.CheckpointEvery {
			if err := doCheckpoint(); err != nil {
				return nil, err
			}
			elemsSinceCheckpoint = 0
		}
	}

	// 流结束：最终检查点，确保全部已确认结果与位点落盘。
	if err := doCheckpoint(); err != nil {
		return nil, err
	}

	return &Result{
		FinalPosition:  position,
		FinalWatermark: tab.Watermark(),
		Metrics:        m,
	}, nil
}
