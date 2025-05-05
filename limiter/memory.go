package limiter

import (
	"context"
	"runtime"
	"sync"
	"time"
)

var defaultRefreshDuration = 100 * time.Millisecond

// FunctionReadMemState 读取内存状态的函数, 方便用于mock
var FunctionReadMemState func(*runtime.MemStats) = runtime.ReadMemStats

// MemoryLimiter 内存限制器
// 用于限制内存使用
type MemoryLimiter struct {
	// 内存限制
	CurrentMemStats *runtime.MemStats
	LimitSizeByte   uint64
	lock            sync.RWMutex
	RefreshDuration time.Duration
	ctx             context.Context
	cancelfunc      context.CancelFunc
}

// NewMemoryLimiter 创建一个内存限制器
func NewMemoryLimiter(limitSizebyte uint64) *MemoryLimiter {

	// new Context
	ctx, cancel := context.WithCancel(context.Background())
	// new limiter
	l := &MemoryLimiter{
		LimitSizeByte:   limitSizebyte,
		CurrentMemStats: &runtime.MemStats{},
		lock:            sync.RWMutex{},
		RefreshDuration: defaultRefreshDuration,
		ctx:             ctx,
		cancelfunc:      cancel,
	}

	return l
}

func (l *MemoryLimiter) SetRefreshDuration(d time.Duration) {
	l.RefreshDuration = d
}

func (l *MemoryLimiter) SetLimitSizeByte(size uint64) {
	l.LimitSizeByte = size
}

func (l *MemoryLimiter) Start() {
	go l.Refresh()
}

func (l *MemoryLimiter) Close() {
	l.cancelfunc()
}

// CheckAvailable 检查系统状态依然是可用状态。
// 如果系统资源达到限制额度，则系统处理不可用状态，返回false，否则返回true
func (l *MemoryLimiter) CheckAvailable() bool {
	// 如果限制大小为0，则不限制
	if l.LimitSizeByte == 0 {
		return true
	}
	currentUsedMemSize := l.GetCurrentUsedMemoryByte()
	return l.LimitSizeByte >= currentUsedMemSize
}

func (l *MemoryLimiter) GetCurrentUsedMemoryByte() uint64 {
	l.lock.RLock()
	defer l.lock.RUnlock()
	return l.CurrentMemStats.HeapSys - l.CurrentMemStats.HeapReleased
}

// Refresh 刷新内存状态
func (l *MemoryLimiter) Refresh() {

	ticker := time.NewTicker(l.RefreshDuration)
	defer ticker.Stop()
	for {
		select {
		case <-l.ctx.Done():
			return
		case <-ticker.C:
			l.lock.Lock()
			FunctionReadMemState(l.CurrentMemStats)
			l.lock.Unlock()
		}
	}
}
