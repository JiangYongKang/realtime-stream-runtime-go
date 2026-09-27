package model

import (
	"strconv"
	"time"
)

// OutputKind 是窗口结果的确定性种类。
type OutputKind int

const (
	// KindFired：窗口关闭时的首次最终结果。
	KindFired OutputKind = iota + 1
	// KindCorrection：允许迟到范围内的迟到事件归并后产生的更正结果。
	KindCorrection
)

func (k OutputKind) String() string {
	switch k {
	case KindFired:
		return "FIRED"
	case KindCorrection:
		return "CORRECTION"
	default:
		return "UNKNOWN(" + strconv.Itoa(int(k)) + ")"
	}
}

// WindowOutput 是向下游投递的一条窗口聚合结果。
// 同一 (WindowStart, Key) 可能产生 FIRED + 若干 CORRECTION，
// 版本号 Version 从 1 起单调递增，下游可据此确定性地归并（后到的同键版本覆盖旧版本）。
type WindowOutput struct {
	WindowStart time.Time
	WindowEnd   time.Time
	Key         string
	Sum         float64
	Count       int64
	Kind        OutputKind
	Version     int
}
