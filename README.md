# realtime-stream-runtime-go

一套**本地流处理运行时**：用一条可重放的输入事件流模拟真实接入，在同一组确定性规则下统一
**事件时间窗口、故障恢复、下游背压**三件事，使结果一致且可解释。

- 语言：Go 1.25，零第三方依赖
- 包结构：
  - `pkg/model` — 事件、窗口结果、排除记录等核心类型
  - `pkg/window` — 滚动窗口、水位线、迟到事件规则、聚合状态
  - `pkg/checkpoint` — 检查点编解码（魔数 + CRC32 + 版本）与原子目录存储
  - `pkg/engine` — 可重放源、有界下游（背压）、运行时主循环
  - `cmd/demo` — 乱序 + 重复 + 超范围迟到 + 崩溃恢复的可复现演示

---

## 1. 窗口与迟到规则

### 时间语义

- 所有时间使用**事件时间**（`Event.EventAt`），窗口边界按 UTC 纪元对齐：
  滚动窗口为左闭右开区间 `[start, end)`，`start = eventAt.Truncate(windowSize)`。
- 水位线（Watermark）由输入流中的 `Watermark` 标记显式推进，**单调不减**；
  回退或原地推进是空操作。恢复重放时旧水位线标记同样是空操作，因此重放确定。

### 窗口生命周期（状态机）

```
OPEN（开放中）
  │  水位线推进到 wm >= 窗口 end
  ▼
FIRED（已触发，结果已可见）           ── 窗口在 end+allowedLateness 之前一直保留
  │  水位线推进到 wm >= end + allowedLateness
  ▼
PURGED（状态已清除）
```

### 一条事件到达时的确定性判定（按顺序）

| 判定 | 结论 | 说明 |
|---|---|---|
| `SeqID` 已处理 | `DUPLICATE` | 重复事件被丢弃，记入排除列表，不参与聚合 |
| 字段非法（空键/零时间/负值） | `DATA_ERROR` | **数据错误**，记入排除列表 |
| 窗口已 `PURGED` | `OUT_OF_RANGE_LATE` | **超范围迟到**：数据合法，但窗口状态已不存在 |
| 窗口 `OPEN` | 吸收 | 普通事件 |
| 窗口 `FIRED` 且仍在保留期内 | 吸收 + `CORRECTION` | 允许范围内的迟到按确定性规则归并 |

关键区分：**`OUT_OF_RANGE_LATE` 是时间语义拒绝，`DATA_ERROR` 是数据质量拒绝**，
二者在排除记录中分别携带 `Reason` 与明细，下游和审计日志中可明确区分，绝不混为一谈。

### 输出与归并规则

- 窗口首次触发时，对每个 key 输出一条 `FIRED`，**版本号固定为 1**；
- 保留期内每到达一条该 key 的迟到事件，累加 `Sum/Count`，版本号 +1，输出一条
  `CORRECTION`（携带该 key 的**完整累计值**而非增量）。下游按
  `(WindowStart, Key, Version)` 归并，后到的高版本覆盖低版本即可；
- 同一水位线推进内的处理顺序固定：**先触发到期窗口（按窗口起点、key 升序），
  再清除超期窗口**。因此任意乱序输入产生完全相同的输出序列。

---

## 2. 检查点与重启恢复语义

### 检查点内容（`checkpoint.State`）

- 格式版本 `Version`、源位点 `Position`（最后已处理元素的事件 `SeqID`）；
- 当前水位线、全部窗口及其 key 聚合（含 `Sum/Count/Version`、是否已 FIRED）；
- 已处理事件的 `SeqID` 去重集合；
- **已确认**的窗口输出列表与排除记录列表。

### 持久化与“已确认”边界

- 写入采用 `临时文件 → fsync → 原子 rename → 目录 fsync`，崩溃只会留下上一份完整检查点，
  不会出现半截可读状态；
- 每次检查点前先 `Sink.Drain()`：检查点只记录**已被下游处理器确认**的结果。
  检查点之后、崩溃之前的结果在崩溃后随重放重新计算并投递，因此
  **重启后输出不重复、不丢失（exactly-once）**；
- 恢复时：重建窗口表 → 用检查点的已确认列表重置下游视图 → 源 `SeekAfter(position)`
  只重放未确认部分。重放中的重复事件被去重集合挡住，已关闭窗口不会重复触发。

### 必须被拒绝的恢复失败（原因可区分，绝不静默继续）

| 情况 | 返回错误 | `errors.Is` 判定 |
|---|---|---|
| 检查点字节损坏（CRC32 不匹配） | `checkpoint.ErrChecksumMismatch` | 是 |
| 魔数错误/截断/载荷无法解析 | `checkpoint.ErrMalformed` | 是 |
| 版本号不受支持 | `checkpoint.ErrVersionIncompatible` | 是 |
| 新检查点位点早于已确认位点 | `checkpoint.ErrPositionRegressed` | 是（提交时拒绝） |
| 目录中没有检查点 | `checkpoint.ErrNoCheckpoint` | 冷启动，从头消费 |

检查点布局：`magic(7B) | payloadLen(4B) | version(4B) | CRC32(4B) | JSON payload`，
校验顺序为 魔数 → 长度 → **CRC → 版本** → 反序列化。编码是确定性的（
map 有序遍历、时间统一 RFC3339Nano），同一状态产生相同字节，便于测试比对。

---

## 3. 背压策略与有界内存

- 下游 `Sink` 以**容量令牌**限制在途结果数（容量 = 最大在途数）。令牌耗尽时，
  处理协程在 `Deliver` 处阻塞——输入读取由此被**节制**，内存使用有界（
  `O(SinkCapacity + 窗口状态)`），不存在无限缓冲队列；
- 单个 worker goroutine 严格按投递顺序调用处理器并确认结果：
  过载消除后输出顺序不变（仍为窗口起点、key 升序），恢复后也不会重复投递；
- `Metrics` 暴露 `BackpressureBlocks`（阻塞次数）与 `BackpressureWaitNanos`（累计等待），
  测试中直接断言过载时节流确实发生。

### 已确认结果的可见性语义

- 处理器 `handler` 返回前，结果不算“已确认”，也不会进入检查点；
- 崩溃只可能发生在检查点边界（见 `CrashHook`），已确认的结果与位点一起原子落盘，
  因此下游看到的是**前缀一致（prefix-consistent）**的可见性：恢复后看到的已确认序列
  是崩溃前序列的前缀延续，没有空洞、没有重复、没有回退。

---

## 4. 本地验证

```bash
# 全部单元测试（带竞态检测）
go test -race ./... -v

# 可复现演示：乱序/重复/超范围迟到/数据错误 + 第 1 个检查点后崩溃重启
go run ./cmd/demo
```

### 测试覆盖（以乱序、重复、中断恢复为主）

| 测试 | 覆盖点 |
|---|---|
| `window.TestNormalWindowFiring` | 乱序事件的正常窗口聚合，FIRED 版本 1 |
| `window.TestLateWithinAllowedProducesCorrection` | 允许范围内迟到 → CORRECTION v2，随后窗口被清除 |
| `window.TestOutOfRangeLateDistinctFromDataError` | 超范围迟到与数据错误严格区分 |
| `window.TestDuplicateSeqID` | 重复 SeqID 被丢弃且不污染聚合 |
| `window.TestSnapshotRestoreRoundTrip` | 快照/恢复后去重与版本连续性 |
| `checkpoint.TestCorruptedBytesRejected` | 字节损坏 → `ErrChecksumMismatch` |
| `checkpoint.TestTruncatedCheckpointRejected` | 截断 → `ErrMalformed` |
| `checkpoint.TestIncompatibleVersionRejected` | 版本不兼容 → `ErrVersionIncompatible` |
| `checkpoint.TestPositionRegressionRejected` | 位点回退 → `ErrPositionRegressed`，等位点幂等允许 |
| `engine.TestRecoveryExactlyOnce` | 崩溃重启后输出与无中断基线逐条一致，排除记录一致 |
| `engine.TestMultipleRestartsRemainExactlyOnce` | 连续三次中断/重启，累计输出仍然不重不丢 |
| `engine.TestCorruptedCheckpointRefusesStartup` / `TestIncompatibleVersionRefusesStartup` | 损坏/版本不兼容的检查点导致启动被拒绝 |
| `engine.TestBackpressureThrottlesAndPreservesOrder` | 持续过载下输入被节流（断言阻塞计数），保序且不重复 |

测试日志会打印关键**事件时间**（如 `watermark=00:16:55`、窗口边界）与
**恢复位点**（`recovering from position=2`、每次检查点的 position），
失败时错误原因以哨兵错误区分（checksum/malformed/version/regression）。

### demo 的期望行为

```
run-1: checkpoint #1 position=2 ... → simulated crash
run-2: recovering from position=2
  FIRED  window=[..10) key=a sum=15 count=2 v1
  FIRED  window=[..10) key=b sum=7  count=1 v1
  CORRECTION window=[..10) key=a sum=18 count=3 v2
  excluded seq=5 OUT_OF_RANGE_LATE   # 超范围迟到（数据合法）
  excluded seq=6 DATA_ERROR          # 空键，数据错误
```
