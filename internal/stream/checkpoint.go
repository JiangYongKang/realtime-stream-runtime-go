package stream

import (
	"bytes"
	"encoding/json"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
)

// 检查点加载失败的可区分原因。
var (
	// ErrCheckpointCorrupt 检查点内容损坏（校验和不匹配或无法解析）。
	ErrCheckpointCorrupt = errors.New("checkpoint corrupt")
	// ErrCheckpointVersion 检查点格式版本与当前运行时不兼容。
	ErrCheckpointVersion = errors.New("checkpoint version incompatible")
	// ErrGenerationIncompatible 检查点代际（lineage）与本次运行配置不一致。
	ErrGenerationIncompatible = errors.New("checkpoint generation incompatible")
	// ErrOffsetRegression 恢复位点早于检查点已确认位点，属于位点回退。
	ErrOffsetRegression = errors.New("offset regression")
	// ErrNoCheckpoint 没有可用的检查点。
	ErrNoCheckpoint = errors.New("no checkpoint")
)

// CheckpointVersion 当前检查点格式版本。
// v2 在 v1 基础上新增 Generation（代际）与 Seen（去重状态）；v1 检查点一律
// 以 ErrCheckpointVersion 拒绝，不做静默迁移。
const CheckpointVersion = 2

// WindowState 窗口聚合状态的可序列化形式。
type WindowState struct {
	Count int64 `json:"count"`
	Sum   int64 `json:"sum"`
}

// Checkpoint 是一次已确认的运行状态快照。
type Checkpoint struct {
	Version      int64                            `json:"version"`
	Generation   string                           `json:"generation"`
	SourceOffset int64                            `json:"sourceOffset"`
	MaxEventTime int64                            `json:"maxEventTime"`
	Started      bool                             `json:"started"`
	NextSeq      int64                            `json:"nextSeq"`
	Windows      map[int64]map[string]WindowState `json:"windows"`
	Seen         []SeenEvent                      `json:"seen"`
}

// envelope 带校验和的磁盘封装。
type envelope struct {
	Payload []byte `json:"payload"`
	CRC     uint32 `json:"crc"`
}

// Store 基于文件的检查点存储。
type Store struct {
	path string
}

// NewStore 构造检查点存储，文件位于 dir/checkpoint.json。
func NewStore(dir string) *Store {
	return &Store{path: filepath.Join(dir, "checkpoint.json")}
}

// Save 原子地写入检查点（先写临时文件再重命名）。
func (s *Store) Save(cp Checkpoint) error {
	payload, err := json.Marshal(cp)
	if err != nil {
		return err
	}
	env := envelope{Payload: payload, CRC: crc32.ChecksumIEEE(payload)}
	data, err := json.Marshal(env)
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}

// Load 读取并校验检查点；损坏、版本不兼容均返回可区分错误。
func (s *Store) Load() (Checkpoint, error) {
	data, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return Checkpoint{}, ErrNoCheckpoint
		}
		return Checkpoint{}, err
	}
	var env envelope
	if err := json.Unmarshal(data, &env); err != nil {
		return Checkpoint{}, ErrCheckpointCorrupt
	}
	if crc32.ChecksumIEEE(env.Payload) != env.CRC {
		return Checkpoint{}, ErrCheckpointCorrupt
	}
	var cp Checkpoint
	dec := json.NewDecoder(bytes.NewReader(env.Payload))
	if err := dec.Decode(&cp); err != nil {
		return Checkpoint{}, ErrCheckpointCorrupt
	}
	if cp.Version != CheckpointVersion {
		return Checkpoint{}, ErrCheckpointVersion
	}
	return cp, nil
}

// Path 返回检查点文件路径（测试用于注入损坏）。
func (s *Store) Path() string { return s.path }
