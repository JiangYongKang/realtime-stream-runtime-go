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
	// ErrCheckpointVersion 检查点版本与当前运行时不兼容。
	ErrCheckpointVersion = errors.New("checkpoint version incompatible")
	// ErrOffsetRegression 恢复位点早于检查点已确认位点，属于位点回退。
	ErrOffsetRegression = errors.New("offset regression")
	// ErrNoCheckpoint 没有可用的检查点。
	ErrNoCheckpoint = errors.New("no checkpoint")
)

// CheckpointVersion 当前检查点格式版本。
// v2：新增去重身份集合与已产出结果历史（全新进程 + 空下游恢复所需）。
// v1 检查点代际不兼容，加载时返回 ErrCheckpointVersion。
const CheckpointVersion = 2

// WindowState 窗口聚合状态的可序列化形式。
type WindowState struct {
	Count int64 `json:"count"`
	Sum   int64 `json:"sum"`
}

// Checkpoint 是一次已确认的运行状态快照。
type Checkpoint struct {
	Version      int64                            `json:"version"`
	SourceOffset int64                            `json:"sourceOffset"`
	MaxEventTime int64                            `json:"maxEventTime"`
	Started      bool                             `json:"started"`
	NextSeq      int64                            `json:"nextSeq"`
	Windows      map[int64]map[string]WindowState `json:"windows"`
	// Dedup 为保留范围内已处理事件的身份集合，恢复后继续识别跨重启的重复投递。
	Dedup []dedupEntry `json:"dedup"`
	// Results 为已对外确认的结果历史，恢复时用于判断回退，并向全新空下游重发。
	Results []Result `json:"results"`
}

// envelope 带校验和的磁盘封装。
type envelope struct {
	Payload []byte `json:"payload"`
	CRC     uint32 `json:"crc"`
}

// Store 基于文件的检查点存储。
type Store struct {
	path   string
	commit string
}

// NewStore 构造检查点存储，检查点位于 dir/checkpoint.json，
// 单调的已确认进度高水位位于 dir/commit.json（跨进程判断进度回退用）。
func NewStore(dir string) *Store {
	return &Store{
		path:   filepath.Join(dir, "checkpoint.json"),
		commit: filepath.Join(dir, "commit.json"),
	}
}

// commitMarker 是独立于下游、只进不退的“已确认进度高水位”。
type commitMarker struct {
	NextSeq int64 `json:"nextSeq"`
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
	return atomicWrite(s.path, data)
}

// Commit 记录单调递增的已确认进度高水位；不允许倒退。
// 崩溃发生在 Save 与 Commit 之间时，只会“提交得偏旧”，下次恢复重放即可，绝不覆盖更新进度。
func (s *Store) Commit(nextSeq int64) error {
	cur, err := s.LoadCommit()
	if err != nil && !errors.Is(err, ErrNoCheckpoint) {
		return err
	}
	if err == nil && nextSeq < cur {
		return ErrOffsetRegression
	}
	data, err := json.Marshal(commitMarker{NextSeq: nextSeq})
	if err != nil {
		return err
	}
	return atomicWrite(s.commit, data)
}

// LoadCommit 读取已确认进度高水位；无标记返回 ErrNoCheckpoint，损坏返回 ErrCheckpointCorrupt。
func (s *Store) LoadCommit() (int64, error) {
	data, err := os.ReadFile(s.commit)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, ErrNoCheckpoint
		}
		return 0, err
	}
	var m commitMarker
	if err := json.Unmarshal(data, &m); err != nil {
		return 0, ErrCheckpointCorrupt
	}
	if m.NextSeq < 0 {
		return 0, ErrCheckpointCorrupt
	}
	return m.NextSeq, nil
}

func atomicWrite(path string, data []byte) error {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
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
