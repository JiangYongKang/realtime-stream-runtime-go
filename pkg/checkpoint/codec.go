package checkpoint

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"strings"
	"time"
)

// 恢复时必须可区分的失败原因，均为哨兵错误，可用 errors.Is 判定：
var (
	// ErrChecksumMismatch：CRC32 校验失败（典型的检查点字节损坏）。
	ErrChecksumMismatch = errors.New("checkpoint checksum mismatch")
	// ErrMalformed：魔数错误/长度不足/载荷无法反序列化。
	ErrMalformed = errors.New("checkpoint malformed")
	// ErrVersionIncompatible：版本字段不受支持（版本不兼容）。
	ErrVersionIncompatible = errors.New("checkpoint version incompatible")
)

const magic = "RSRCKP\x01"

// envelope 布局（小端）：
//
//	[0:7]   magic (7 bytes)
//	[7:11]  payload length uint32
//	[11:15] version uint32
//	[15:19] CRC32 IEEE over (version bytes ++ payload)
//	[19..]  JSON payload
const (
	headerLen  = 19
	crcTableAt = 15
	verAt      = 11
	lenAt      = 7
)

// jsonDTO 是 State 的 JSON 表示。Go 的 encoding/json 不支持结构体作为 map 键，
// 因此窗口聚合键编码为 "RFC3339Nano|key"；字段声明序即 JSON 键序，配合上游的
// 有序遍历，同一逻辑状态编码出完全相同的字节（确定性快照）。
type jsonDTO struct {
	Version    int                       `json:"version"`
	Position   int64                     `json:"position"`
	Watermark  string                    `json:"watermark"`
	WindowMeta map[string]jsonWindowMeta `json:"window_meta"`
	Windows    map[string]jsonAgg        `json:"windows"`
	Seen       []int64                   `json:"seen"`
	Outputs    []jsonOutput              `json:"outputs"`
	Exclusions []jsonExclusion           `json:"exclusions"`
}

type jsonWindowMeta struct {
	End   string `json:"end"`
	Fired bool   `json:"fired"`
}

type jsonAgg struct {
	Sum     float64 `json:"sum"`
	Count   int64   `json:"count"`
	Version int     `json:"version"`
}

type jsonOutput struct {
	WindowStart string  `json:"window_start"`
	WindowEnd   string  `json:"window_end"`
	Key         string  `json:"key"`
	Sum         float64 `json:"sum"`
	Count       int64   `json:"count"`
	Kind        int     `json:"kind"`
	Version     int     `json:"version"`
}

type jsonExclusion struct {
	SeqID      int64  `json:"seq_id"`
	Key        string `json:"key"`
	EventAt    string `json:"event_at"`
	Reason     int    `json:"reason"`
	Detail     string `json:"detail"`
	ObservedAt string `json:"observed_at"`
}

func ts(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }

func pts(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, nil
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}, err
	}
	return t.UTC(), nil
}

func winKey(start time.Time, key string) string { return ts(start) + "|" + key }

func parseWinKey(k string) (time.Time, string, error) {
	idx := strings.IndexByte(k, '|')
	if idx < 0 {
		return time.Time{}, "", fmt.Errorf("bad window key %q", k)
	}
	t, err := pts(k[:idx])
	if err != nil {
		return time.Time{}, "", err
	}
	return t, k[idx+1:], nil
}

func toDTO(s *State) (jsonDTO, error) {
	d := jsonDTO{
		Version:    s.Version,
		Position:   s.Position,
		Watermark:  ts(s.Watermark),
		WindowMeta: make(map[string]jsonWindowMeta, len(s.WindowMeta)),
		Windows:    make(map[string]jsonAgg, len(s.Windows)),
		Seen:       s.Seen,
		Outputs:    make([]jsonOutput, 0, len(s.Outputs)),
		Exclusions: make([]jsonExclusion, 0, len(s.Exclusions)),
	}
	for st, m := range s.WindowMeta {
		d.WindowMeta[ts(st)] = jsonWindowMeta{End: ts(m.End), Fired: m.Fired}
	}
	for wk, a := range s.Windows {
		d.Windows[winKey(wk.WindowStart, wk.Key)] = jsonAgg{Sum: a.Sum, Count: a.Count, Version: a.Version}
	}
	for _, o := range s.Outputs {
		d.Outputs = append(d.Outputs, jsonOutput{
			WindowStart: ts(o.WindowStart), WindowEnd: ts(o.WindowEnd), Key: o.Key,
			Sum: o.Sum, Count: o.Count, Kind: o.Kind, Version: o.Version,
		})
	}
	for _, x := range s.Exclusions {
		d.Exclusions = append(d.Exclusions, jsonExclusion{
			SeqID: x.SeqID, Key: x.Key, EventAt: ts(x.EventAt),
			Reason: x.Reason, Detail: x.Detail, ObservedAt: ts(x.ObservedAt),
		})
	}
	return d, nil
}

func fromDTO(d jsonDTO) (*State, error) {
	wm, err := pts(d.Watermark)
	if err != nil {
		return nil, err
	}
	s := &State{
		Version:    d.Version,
		Position:   d.Position,
		Watermark:  wm,
		WindowMeta: make(map[time.Time]WindowMeta, len(d.WindowMeta)),
		Windows:    make(map[WinKey]AggState, len(d.Windows)),
		Seen:       d.Seen,
		Outputs:    make([]OutputRecord, 0, len(d.Outputs)),
		Exclusions: make([]ExclusionRecord, 0, len(d.Exclusions)),
	}
	for k, m := range d.WindowMeta {
		st, err := pts(k)
		if err != nil {
			return nil, err
		}
		end, err := pts(m.End)
		if err != nil {
			return nil, err
		}
		s.WindowMeta[st] = WindowMeta{End: end, Fired: m.Fired}
	}
	for k, a := range d.Windows {
		st, key, err := parseWinKey(k)
		if err != nil {
			return nil, err
		}
		s.Windows[WinKey{WindowStart: st, Key: key}] = AggState{
			Sum: a.Sum, Count: a.Count, Version: a.Version,
		}
	}
	for _, o := range d.Outputs {
		st, err := pts(o.WindowStart)
		if err != nil {
			return nil, err
		}
		end, err := pts(o.WindowEnd)
		if err != nil {
			return nil, err
		}
		s.Outputs = append(s.Outputs, OutputRecord{
			WindowStart: st, WindowEnd: end, Key: o.Key,
			Sum: o.Sum, Count: o.Count, Kind: o.Kind, Version: o.Version,
		})
	}
	for _, x := range d.Exclusions {
		ea, err := pts(x.EventAt)
		if err != nil {
			return nil, err
		}
		oa, err := pts(x.ObservedAt)
		if err != nil {
			return nil, err
		}
		s.Exclusions = append(s.Exclusions, ExclusionRecord{
			SeqID: x.SeqID, Key: x.Key, EventAt: ea,
			Reason: x.Reason, Detail: x.Detail, ObservedAt: oa,
		})
	}
	return s, nil
}

// Encode 将状态确定性地序列化为带魔数、版本与校验和的字节。
func Encode(s *State) ([]byte, error) {
	if s == nil {
		return nil, errors.New("cannot encode nil state")
	}
	if s.Version != Version {
		return nil, fmt.Errorf("%w: got %d want %d", ErrVersionIncompatible, s.Version, Version)
	}
	dto, err := toDTO(s)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrMalformed, err)
	}
	payload, err := json.Marshal(dto)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrMalformed, err)
	}

	buf := make([]byte, headerLen+len(payload))
	copy(buf[:len(magic)], magic)
	binary.LittleEndian.PutUint32(buf[lenAt:lenAt+4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(buf[verAt:verAt+4], uint32(s.Version))
	copy(buf[headerLen:], payload)

	h := crc32.NewIEEE()
	_, _ = h.Write(buf[verAt:crcTableAt])
	_, _ = h.Write(payload)
	binary.LittleEndian.PutUint32(buf[crcTableAt:crcTableAt+4], h.Sum32())
	return buf, nil
}

// Decode 按顺序校验魔数、长度、校验和与版本后反序列化。
// 失败原因严格区分：ErrMalformed（结构问题）/ ErrChecksumMismatch（损坏）/
// ErrVersionIncompatible（版本不兼容）。调用方绝不能从中途静默继续。
func Decode(data []byte) (*State, error) {
	if len(data) < headerLen || string(data[:len(magic)]) != magic {
		return nil, fmt.Errorf("%w: bad magic or truncated header", ErrMalformed)
	}
	plen := binary.LittleEndian.Uint32(data[lenAt : lenAt+4])
	if int(plen) != len(data)-headerLen {
		return nil, fmt.Errorf("%w: declared length %d but body is %d bytes",
			ErrMalformed, plen, len(data)-headerLen)
	}
	version := binary.LittleEndian.Uint32(data[verAt : verAt+4])
	payload := data[headerLen:]
	gotSum := binary.LittleEndian.Uint32(data[crcTableAt : crcTableAt+4])
	ch := crc32.NewIEEE()
	_, _ = ch.Write(data[verAt:crcTableAt])
	_, _ = ch.Write(payload)
	wantSum := ch.Sum32()
	if gotSum != wantSum {
		return nil, fmt.Errorf("%w: stored=%08x computed=%08x", ErrChecksumMismatch, gotSum, wantSum)
	}
	if version != Version {
		return nil, fmt.Errorf("%w: got %d want %d", ErrVersionIncompatible, version, Version)
	}
	var dto jsonDTO
	if err := json.Unmarshal(payload, &dto); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrMalformed, err)
	}
	if dto.Version != Version {
		return nil, fmt.Errorf("%w: payload version %d", ErrVersionIncompatible, dto.Version)
	}
	st, err := fromDTO(dto)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrMalformed, err)
	}
	return st, nil
}
