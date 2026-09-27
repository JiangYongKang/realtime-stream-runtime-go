package checkpoint

import (
	"errors"
	"testing"
	"time"
)

var base = time.Unix(1_000, 0).UTC()

func sampleState(pos int64) *State {
	return &State{
		Version:   Version,
		Position:  pos,
		Watermark: base.Add(10 * time.Second),
		WindowMeta: map[time.Time]WindowMeta{
			base: {End: base.Add(10 * time.Second), Fired: true},
		},
		Windows: map[WinKey]AggState{
			{WindowStart: base, Key: "a"}: {Sum: 15, Count: 2, Version: 1},
		},
		Seen: []int64{1, 2, 3},
		Outputs: []OutputRecord{
			{WindowStart: base, WindowEnd: base.Add(10 * time.Second), Key: "a",
				Sum: 15, Count: 2, Kind: 1, Version: 1},
		},
		Exclusions: []ExclusionRecord{
			{SeqID: 9, Key: "a", EventAt: base.Add(2 * time.Second), Reason: 2,
				Detail: "late", ObservedAt: base.Add(11 * time.Second)},
		},
	}
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	st := sampleState(42)
	data, err := Encode(st)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	back, err := Decode(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if back.Position != 42 || !back.Watermark.Equal(st.Watermark) {
		t.Fatalf("round trip mismatch: %+v", back)
	}
	if got := back.Windows[WinKey{WindowStart: base, Key: "a"}]; got.Sum != 15 || got.Version != 1 {
		t.Fatalf("window state mismatch: %+v", got)
	}
	// 确定性：同一状态编码两次字节必须一致。
	data2, err := Encode(st)
	if err != nil {
		t.Fatalf("encode 2: %v", err)
	}
	if string(data) != string(data2) {
		t.Fatal("encoding is not deterministic")
	}
}

func TestNoCheckpointIsColdStart(t *testing.T) {
	s, err := NewDirStore(t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if _, err := s.Load(); !errors.Is(err, ErrNoCheckpoint) {
		t.Fatalf("expected ErrNoCheckpoint, got %v", err)
	}
}

func TestCorruptedBytesRejected(t *testing.T) {
	dir := t.TempDir()
	s, err := NewDirStore(dir)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if err := s.Save(sampleState(7)); err != nil {
		t.Fatalf("save: %v", err)
	}
	if err := s.Corrupt("bytes"); err != nil {
		t.Fatalf("corrupt: %v", err)
	}
	_, err = s.Load()
	if !errors.Is(err, ErrChecksumMismatch) {
		t.Fatalf("expected ErrChecksumMismatch, got %v", err)
	}
	t.Logf("corrupted checkpoint at position=7 rejected: %v", err)
}

func TestTruncatedCheckpointRejected(t *testing.T) {
	dir := t.TempDir()
	s, err := NewDirStore(dir)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if err := s.Save(sampleState(7)); err != nil {
		t.Fatalf("save: %v", err)
	}
	if err := s.Corrupt("truncate"); err != nil {
		t.Fatalf("corrupt: %v", err)
	}
	_, err = s.Load()
	if !errors.Is(err, ErrMalformed) {
		t.Fatalf("expected ErrMalformed, got %v", err)
	}
	t.Logf("truncated checkpoint rejected: %v", err)
}

func TestIncompatibleVersionRejected(t *testing.T) {
	dir := t.TempDir()
	s, err := NewDirStore(dir)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if err := s.Save(sampleState(7)); err != nil {
		t.Fatalf("save: %v", err)
	}
	if err := s.Corrupt("version"); err != nil {
		t.Fatalf("corrupt: %v", err)
	}
	_, err = s.Load()
	if !errors.Is(err, ErrVersionIncompatible) {
		t.Fatalf("expected ErrVersionIncompatible, got %v", err)
	}
	t.Logf("incompatible version rejected: %v", err)
}

func TestPositionRegressionRejected(t *testing.T) {
	s, err := NewDirStore(t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if err := s.Save(sampleState(10)); err != nil {
		t.Fatalf("save 10: %v", err)
	}
	err = s.Save(sampleState(4))
	if !errors.Is(err, ErrPositionRegressed) {
		t.Fatalf("expected ErrPositionRegressed, got %v", err)
	}
	t.Logf("position regression 10 -> 4 rejected: %v", err)
	// 等位点允许（幂等重试），且原检查点未被破坏。
	if err := s.Save(sampleState(10)); err != nil {
		t.Fatalf("idempotent retry at same position should pass: %v", err)
	}
	pos, err := s.LastPosition()
	if err != nil || pos != 10 {
		t.Fatalf("last position = %d, %v", pos, err)
	}
}
