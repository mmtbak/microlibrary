package batchcache

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestBatchCache_ConcurrencyControl(t *testing.T) {
	var (
		maxConcurrent = 2
		current       int32
		peak          int32
		mu            sync.Mutex
		wg            sync.WaitGroup
	)

	// 模拟耗时处理函数
	processor := func(batch []string) {
		defer wg.Done()

		n := atomic.AddInt32(&current, 1)
		defer atomic.AddInt32(&current, -1)

		mu.Lock()
		if n > peak {
			peak = n
		}
		mu.Unlock()

		time.Sleep(100 * time.Millisecond) // 模拟处理耗时
	}

	bc := New[string](2, 50*time.Millisecond, maxConcurrent, processor)
	bc.Start()
	defer bc.Stop()

	// 发送6个批次(应该会被限制为最多2个并发)
	wg.Add(6)
	for i := 0; i < 6; i++ {
		bc.Add("a")
		bc.Add("b")
		time.Sleep(10 * time.Millisecond)
	}

	wg.Wait()

	if peak > int32(maxConcurrent) {
		t.Errorf("expected max %d concurrent flushes, got %d", maxConcurrent, peak)
	}
}

func TestBatchCache_StopWaitsForCompletion(t *testing.T) {
	var (
		processed int
		done      = make(chan struct{})
	)

	processor := func(batch []float64) {
		time.Sleep(200 * time.Millisecond) // 模拟耗时处理
		processed = len(batch)
		close(done)
	}

	bc := New[float64](1, time.Second, 0, processor)
	bc.Start()

	bc.Add(1.0)
	bc.Stop() // 应该等待处理完成

	select {
	case <-done:
		if processed != 1 {
			t.Errorf("expected processed 1 item, got %d", processed)
		}
	default:
		t.Error("Stop() returned before processor completed")
	}
}

func TestBatchCache_Metrics(t *testing.T) {
	processor := func(batch []rune) {}

	bc := New[rune](5, time.Second, 0, processor)
	bc.Start()
	defer bc.Stop()

	bc.Add('a')
	bc.Add('b')

	if bc.QueueLength() != 2 {
		t.Errorf("expected queue length 2, got %d", bc.QueueLength())
	}

	bc.Add('c')
	bc.Add('d')
	bc.Add('e') // 触发刷新

	time.Sleep(10 * time.Millisecond) // 让goroutine有时间处理
	if bc.QueueLength() != 0 {
		t.Errorf("expected queue length 0 after flush, got %d", bc.QueueLength())
	}
}
func BenchmarkBatchCache(b *testing.B) {
	processor := func(batch []int) {}

	bc := New(1000, time.Minute, 0, processor)
	bc.Start()
	defer bc.Stop()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bc.Add(i)
	}
}

func BenchmarkBatchCacheWithConcurrencyLimit(b *testing.B) {
	processor := func(batch []int) {
		time.Sleep(time.Millisecond) // 模拟处理耗时
	}

	bc := New[int](100, 10*time.Millisecond, 4, processor)
	bc.Start()
	defer bc.Stop()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bc.Add(i)
	}
	b.StopTimer()
}
