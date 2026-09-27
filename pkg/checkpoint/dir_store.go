package checkpoint

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
)

const (
	fileName = "checkpoint.dat"
	tmpName  = "checkpoint.tmp"
)

// DirStore 是基于本地目录的原子检查点存储。
// 提交采用“写临时文件 + fsync + 原子 rename”，崩溃只会留下上一份完整检查点，
// 绝不会出现半截可读的状态。
type DirStore struct {
	dir          string
	lastPosition int64
	havePos      bool
}

// NewDirStore 创建/打开一个目录检查点存储。
func NewDirStore(dir string) (*DirStore, error) {
	if dir == "" {
		return nil, errors.New("checkpoint dir must not be empty")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create checkpoint dir: %w", err)
	}
	s := &DirStore{dir: dir}
	if st, err := s.Load(); err == nil {
		s.lastPosition = st.Position
		s.havePos = true
	} else if errors.Is(err, ErrNoCheckpoint) {
		// 冷启动：没有位点基线。
	} else {
		// 打开时若已有检查点损坏，立即暴露，不静默忽略。
		return nil, err
	}
	return s, nil
}

// Save 原子提交一个检查点。位点必须单调不减：
// 回退（新位点早于已确认位点）以 ErrPositionRegressed 拒绝，防止旧状态覆盖新状态。
func (s *DirStore) Save(state *State) error {
	if state == nil {
		return errors.New("cannot save nil state")
	}
	if s.havePos && state.Position < s.lastPosition {
		return fmt.Errorf("%w: new position %d < committed %d",
			ErrPositionRegressed, state.Position, s.lastPosition)
	}
	data, err := Encode(state)
	if err != nil {
		return err
	}

	final := filepath.Join(s.dir, fileName)
	tmp := filepath.Join(s.dir, tmpName)
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("create temp checkpoint: %w", err)
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return fmt.Errorf("write temp checkpoint: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("fsync temp checkpoint: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close temp checkpoint: %w", err)
	}
	if err := os.Rename(tmp, final); err != nil {
		return fmt.Errorf("commit checkpoint: %w", err)
	}
	// 同步目录项，保证 rename 在崩溃后可见。
	if d, err := os.Open(s.dir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}

	s.lastPosition = state.Position
	s.havePos = true
	return nil
}

// Load 读取最近一次已确认检查点；不存在返回 ErrNoCheckpoint（冷启动），
// 损坏/版本不兼容返回对应哨兵错误包裹的错误，调用方必须中止启动。
func (s *DirStore) Load() (*State, error) {
	data, err := os.ReadFile(filepath.Join(s.dir, fileName))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrNoCheckpoint
		}
		return nil, fmt.Errorf("read checkpoint: %w", err)
	}
	return Decode(data)
}

// LastPosition 返回最近一次已确认位点；无检查点时返回 ErrNoCheckpoint。
func (s *DirStore) LastPosition() (int64, error) {
	if !s.havePos {
		return 0, ErrNoCheckpoint
	}
	return s.lastPosition, nil
}

// Corrupt 仅供测试，按 reason 注入不同类型的损坏：
//   - "bytes"（默认）：翻转载荷字节 → 恢复时 ErrChecksumMismatch；
//   - "truncate"：截断文件 → ErrMalformed；
//   - "version"：改写版本号并重算校验和 → ErrVersionIncompatible。
func (s *DirStore) Corrupt(reason string) error {
	final := filepath.Join(s.dir, fileName)
	switch reason {
	case "", "bytes":
		data, err := os.ReadFile(final)
		if err != nil {
			if os.IsNotExist(err) {
				return ErrNoCheckpoint
			}
			return err
		}
		if len(data) > headerLen+4 {
			data[headerLen+4] ^= 0xFF
		}
		return os.WriteFile(final, data, 0o644)
	case "truncate":
		data, err := os.ReadFile(final)
		if err != nil {
			if os.IsNotExist(err) {
				return ErrNoCheckpoint
			}
			return err
		}
		cut := headerLen
		if len(data) < cut {
			cut = len(data) / 2
		}
		return os.WriteFile(final, data[:cut], 0o644)
	case "version":
		data, err := os.ReadFile(final)
		if err != nil {
			if os.IsNotExist(err) {
				return ErrNoCheckpoint
			}
			return err
		}
		if len(data) < headerLen {
			return ErrMalformed
		}
		// Encode 会拒绝非当前版本，因此直接改写版本字段并重算 CRC，
		// 得到一个结构完好、校验通过但版本不兼容的检查点。
		cp := make([]byte, len(data))
		copy(cp, data)
		binary.LittleEndian.PutUint32(cp[verAt:verAt+4], uint32(Version+99))
		h := crc32.NewIEEE()
		_, _ = h.Write(cp[verAt:crcTableAt])
		_, _ = h.Write(cp[headerLen:])
		binary.LittleEndian.PutUint32(cp[crcTableAt:crcTableAt+4], h.Sum32())
		return os.WriteFile(final, cp, 0o644)
	default:
		return fmt.Errorf("unknown corruption reason %q", reason)
	}
}
