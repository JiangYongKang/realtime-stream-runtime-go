package checkpoint

import "errors"

// ErrNoCheckpoint 表示尚不存在任何已确认检查点（冷启动，不是损坏）。
var ErrNoCheckpoint = errors.New("no checkpoint exists")

// ErrPositionRegressed：新检查点位点早于已确认位点（位点回退，拒绝提交）。
var ErrPositionRegressed = errors.New("checkpoint position regressed")

// Store 是检查点的持久化存储抽象。
type Store interface {
	// Save 原子提交一个检查点；位点回退必须被拒绝。
	Save(state *State) error
	// Load 读取最近一次已确认检查点。
	Load() (*State, error)
	// LastPosition 返回最近一次已确认位点。
	LastPosition() (int64, error)
	// Corrupt 仅供测试：用损坏内容覆盖检查点。
	Corrupt(reason string) error
}
