# realtime-stream-runtime-go

本地流处理运行时：用可重放的输入事件流模拟真实接入，在同一组规则下处理
**事件时间窗口、故障恢复与下游背压**，给出一致且可解释的结果。

## 结构

```
internal/stream/
  event.go       事件模型与可重放 Source（SliceSource，支持 SeekOffset 重放）
  watermark.go   水位线跟踪：watermark = maxEventTime - allowedLateness
  window.go      事件时间滚动窗口、确定性归并、超范围迟到记录
  checkpoint.go  检查点格式、CRC 校验、原子落盘、可区分的恢复错误
  sink.go        有界缓冲 Sink：背压、确认可见性、未确认截断
  runtime.go     编排：Source -> 水位线/窗口 -> Sink，检查点与恢复
cmd/demo/        本地演示：乱序 + 迟到 + 崩溃恢复 + 背压
```

## 窗口与迟到规则

- 窗口为事件时间滚动窗口，宽度 `windowSize`；水位线
  `watermark = maxEventTime - allowedLateness`。
- 窗口 `[start, end)` 在 `watermark >= end` 时关闭并产出聚合结果
  （count/sum），输出按 `(windowStart, key)` 确定性排序，序号 `Seq` 全局递增。
- **允许范围内的迟到**（事件时间 ≤ watermark 但所属窗口尚未关闭）：
  与正常事件走同一条 `Add` 路径确定性归并，乱序输入与顺序输入结果一致。
- **超范围迟到**（所属窗口已关闭）：不再吸收，记入 `Runtime.LateDropped`
  （含事件时间、窗口区间、当前水位线与原因），与数据错误区分。
- **数据错误**（如空 key）：记入 `Runtime.DataErrors`，错误为
  `ErrDataCorrupt`，与迟到丢弃完全分开统计。

## 恢复语义

- `Checkpoint()` 持久化：源位点、最大事件时间、窗口状态、下一输出序号，
  并将已产出结果确认为对外可见（`Sink.ConfirmUpTo`）。
- `Restore()` 从最近检查点恢复：源 `SeekOffset` 回退重放，窗口状态还原，
  Sink 中**未确认结果被截断**（视为未发生），由重放确定性地产出相同结果——
  从任一已确认检查点重启后输出不重复、不丢失。
- 恢复失败原因可区分，绝不从中途静默继续：
  - `ErrCheckpointCorrupt`：CRC 校验失败或内容无法解析；
  - `ErrCheckpointVersion`：检查点版本与运行时不兼容；
  - `ErrOffsetRegression`：检查点落后于已确认输出，或源位点越界；
  - `ErrNoCheckpoint`：无可用检查点。

## 背压策略

- Sink 缓冲有界（`NewSink(cap)`）；缓冲满时 `Write` 阻塞，
  单线程处理循环随之停顿，**输入读取被节制而非无限缓冲**，内存占用有界。
- 下游通过 `ConfirmUpTo(seq)` 确认释放缓冲；恢复确认后输出严格按
  `Seq` 递增，无乱序、无重复投递。
- 已确认结果立即可见且不可撤销；未确认结果在重启时被截断重放，
  可见性语义在恢复前后一致。
- `Write` 支持 `context` 取消，持续过载下取消不会泄漏阻塞。

## 本地验证

```bash
go test ./internal/stream/ -v   # 单测：日志打印关键事件时间与恢复位点
go run ./cmd/demo               # 端到端演示：检查点 -> 崩溃 -> 恢复 -> 输出
```

测试覆盖：正常窗口聚合、允许范围内迟到归并、超范围迟到记录、乱序确定性、
检查点损坏 / 版本不兼容 / 位点回退（原因可区分）、中断恢复一致性、
持续背压下的有界内存与有序输出、背压下的取消。
