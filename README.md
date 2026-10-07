# realtime-stream-runtime-go

本地流处理运行时：用可重放的输入事件流模拟真实接入，在同一组规则下处理
**事件时间窗口、至少一次投递下的幂等去重、跨进程故障恢复与下游背压**，
给出一致且可解释的结果。

## 结构

```
internal/stream/
  event.go       事件模型（含身份 ID）与可重放 Source（SliceSource，支持 SeekOffset）
  watermark.go   水位线跟踪：watermark = maxEventTime - allowedLateness
  window.go      事件时间滚动窗口、确定性归并、超范围迟到记录
  dedup.go       按事件 ID 的幂等去重：重复/冲突识别、事件时间保留范围回收（有界）
  checkpoint.go  检查点 v2 格式（含代际与去重状态）、CRC、原子落盘、可区分错误
  sink.go        有界缓冲 Sink：背压、确认可见性、未确认截断
  runtime.go     编排：Source -> 去重/水位线/窗口 -> Sink，检查点与两种恢复路径
cmd/demo/        本地演示：重复 + 乱序 + 迟到 + 全新进程恢复 + 背压
```

## 用法

```go
cfg := stream.Config{
    WindowSize:      10,    // 滚动窗口宽度（毫秒）
    AllowedLateness: 5,     // 允许迟到范围（毫秒）
    DedupRetention:  60,    // 去重状态事件时间保留范围（毫秒）；0 = 不回收
    Generation:      "lineage-1", // 处理逻辑代际，恢复时强校验
}
rt := stream.NewWithConfig(src, cfg, sink, store)

// 正常跑（结束自动 Flush + Checkpoint）
rt.Run(ctx)

// 或恢复：根据下游状态自动选择路径
rt.Restore()
rt.Run(ctx)
```

旧构造方式 `stream.New(src, windowSize, allowedLateness, sink, store)` 保留，
等价于 `DedupRetention=0`、`Generation=""` 的配置（此时不设 ID 的事件行为与旧版完全一致）。

## 重复投递幂等

- 事件新增 `ID` 身份字段。`ID` 非空时按身份去重：同 ID 且内容
  （key/eventTime/value）完全一致判定为**重复投递**，不计入聚合、不推进水位线；
  同 ID 但内容不一致判定为**数据冲突**（`ErrDataCorrupt`），不静默吞掉。
  `ID` 为空的事件不参与去重，按旧语义处理。
- 识别先于窗口处理，但**登记（commit）推迟到事件确定被吸收之后**：因此
  检查点保存的已见集合恰好只覆盖已消费位点，崩溃后重放未检查点的事件不会
  被误判成重复。重复紧挨出现、与正常事件任意乱序、以任意次数混入，最终
  每个窗口每个 key 的聚合都与完全无重复的输入逐条一致。
- 三类异常分开统计、互不混淆：
  `Runtime.Duplicates`（重复投递）、`Runtime.LateDropped`（超范围迟到）、
  `Runtime.DataErrors`（数据错误/冲突）。

## 跨进程恢复（全新进程 + 空下游）

`Restore()` 先加载并校验检查点，再按下游实际状态选择路径：

- **下游为空（全新进程、全新空 Sink，只有落盘状态与可重放输入）**：
  从源位点 `0` **全量确定性重放**。已对外可见的历史结果在新下游上逐条重新
  变得可见，后续结果按确定顺序接上；整体输出与从头不中断跑的基线逐条一致，
  不重复也不丢。检查点在这里只作为“同代际合法延续”的门槛。
- **下游保留着已确认结果（同进程续跑）**：源位点回退到检查点、窗口/水位/
  去重状态还原，未确认结果截断，由重放确定性重新产出。

恢复失败原因全部可区分，绝不带病从中途静默继续：

- `ErrCheckpointCorrupt`：CRC 校验失败或内容无法解析；
- `ErrCheckpointVersion`：检查点格式版本不兼容（**v1 老检查点一律拒绝，
  不做静默迁移**，当前为 v2）；
- `ErrGenerationIncompatible`：检查点 `Generation` 与本次运行配置不一致
  （换了处理逻辑/聚合口径的代际）；
- `ErrOffsetRegression`：检查点进度落后于下游已确认结果（会覆盖/倒退已可见
  结果），或源位点越界；
- `ErrNoCheckpoint`：无可用检查点。

### 与老检查点/老数据的兼容边界

- 检查点 v2 在 v1 基础上新增 `generation` 与 `seen`（去重状态）字段。
  v1 落盘被加载时以 `ErrCheckpointVersion` 明确拒绝，需要用可重放输入
  从头冷启动，不支持原地升级续跑。
- `Generation` 相同才允许恢复；改变窗口/聚合/去重语义时应更换代际标识。
- 老输入事件不带 `ID` 时不做去重，窗口、水位、迟到语义保持不变。

## 有界状态与背压

- 去重状态按 **事件时间保留范围** `DedupRetention` 回收：当
  `watermark - eventTime >= retention` 时淘汰对应已见 ID（最小堆驱动）。
  落在保留范围内的 ID 一定保留，该去重的照样去重；`Runtime.DedupEvictions`
  暴露累计回收条数，`Runtime.DedupSeen()` 暴露当前集合大小。`retention=0`
  是显式选择的“永不回收”。
- 窗口状态在关闭后即删除，水位线为标量；恢复相关内存不随运行时间无限增长。
- Sink 缓冲有界（`NewSink(cap)`），满时 `Write` 阻塞，处理循环与输入读取
  随之停顿。**在空下游/慢下游持续不消费时做全量重放，内存同样被背压约束**，
  不会因重放而被撑爆；下游恢复确认后输出严格按 `Seq` 连续递增。

## 本地验证

```bash
go test ./internal/stream/ -v   # 单测：日志打印关键事件时间、水位与恢复位点
go run ./cmd/demo               # 端到端：检查点 -> 换进程 -> 空下游重建 -> 输出
```

测试覆盖：

- 窗口聚合、允许范围内迟到归并、超范围迟到记录、乱序确定性；
- **重复投递幂等**（重复紧挨/乱序/多次混入与无重复基线一致、重复与数据错误/
  迟到分开统计、同 ID 冲突按数据错误处理）；
- **全新进程 + 空下游恢复**（全量重放输出与基线逐条一致、Seq 连续不重不丢）；
- **拒绝恢复**：检查点损坏、v1 老格式/版本不兼容、代际不匹配、进度回退/位点越界；
- **有界状态**：长时间运行下去重集合随保留回收、保留范围内仍去重、
  检查点去重状态有界、空/慢下游重放背压下内存有界；
- 持续背压下的有序输出与取消。
