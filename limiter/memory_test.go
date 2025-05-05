package limiter

import (
	"runtime"
	"testing"
	"time"

	"gopkg.in/go-playground/assert.v1"
)

func TestLimiter(t *testing.T) {
	FunctionReadMemState = MockReadMemState
	limiter := NewMemoryLimiter(1024 * 1024 * 1024 * 1)
	limiter.SetRefreshDuration(100 * time.Millisecond)
	limiter.Start()
	time.Sleep(1 * time.Second)

	isavailable := limiter.CheckAvailable()
	assert.Equal(t, isavailable, true)

	usedmemory := limiter.GetCurrentUsedMemoryByte()
	assert.Equal(t, usedmemory, uint64(1024*1024*1024*1))

	limiter.SetLimitSizeByte(1024 * 1024 * 1)
	isavailable = limiter.CheckAvailable()
	assert.Equal(t, isavailable, false)

	usedmemory = limiter.GetCurrentUsedMemoryByte()
	assert.Equal(t, usedmemory, uint64(1024*1024*1024*1))

	limiter.Close()
}

// MockReadMemState 模拟读取内存状态
func MockReadMemState(m *runtime.MemStats) {
	m.HeapAlloc = 1024 * 1024 * 1024 * 2
	m.HeapSys = 1024 * 1024 * 1024 * 2
	m.HeapIdle = 1024 * 1024 * 1024 * 2
	m.HeapInuse = 1024 * 1024 * 1024 * 2
	m.HeapReleased = 1024 * 1024 * 1024 * 1
	m.HeapObjects = 1024 * 1024 * 1024 * 2
	m.HeapIdle = 1024 * 1024 * 1024 * 2
	m.HeapInuse = 1024 * 1024 * 1024 * 2
}
