package batchcache

import (
	"sync"
	"sync/atomic"
	"time"
)

type BatchCache[T any] struct {
	maxItems         int           // 最大缓存项数
	flushInterval    time.Duration // 刷新间隔
	maxConcurrent    int           // 最大并发处理数 (0表示无限制)
	buffer           []T           // 数据缓冲区
	processor        func([]T)     // 批处理函数
	stopChan         chan struct{} // 停止信号
	wg               sync.WaitGroup
	mutex            sync.Mutex
	flushTimer       *time.Timer
	flushing         atomic.Bool    // 刷新状态标记
	freshTriggerChan chan struct{}  // 触发通道(缓冲为1)
	semaphore        chan struct{}  // 并发控制信号量
	pendingWg        sync.WaitGroup // 等待处理中的任务
	metrics          struct {       // 内置监控指标
		queueLength int64
		activeFlush int64
	}
}

func New[T any](
	maxItems int,
	flushInterval time.Duration,
	maxConcurrent int,
	processor func([]T),
) *BatchCache[T] {
	bc := &BatchCache[T]{
		maxItems:         maxItems,
		flushInterval:    flushInterval,
		maxConcurrent:    maxConcurrent,
		buffer:           make([]T, 0, maxItems),
		processor:        processor,
		stopChan:         make(chan struct{}),
		freshTriggerChan: make(chan struct{}, 1), // 缓冲为1的通知通道
		flushTimer:       time.NewTimer(flushInterval),
	}
	bc.flushTimer.Stop()

	if maxConcurrent > 0 {
		bc.semaphore = make(chan struct{}, maxConcurrent)
		for i := 0; i < maxConcurrent; i++ {
			bc.semaphore <- struct{}{}
		}
	}

	return bc
}

func (bc *BatchCache[T]) Start() {
	bc.wg.Add(1)
	go bc.run()
}

func (bc *BatchCache[T]) Stop() {
	close(bc.stopChan)
	bc.wg.Wait()
	bc.pendingWg.Wait() // 等待所有处理完成
}

func (bc *BatchCache[T]) Add(item T) {
	bc.mutex.Lock()
	bc.buffer = append(bc.buffer, item)
	needFlush := len(bc.buffer) >= bc.maxItems
	atomic.StoreInt64(&bc.metrics.queueLength, int64(len(bc.buffer)))
	bc.mutex.Unlock()

	// 只有达到上限时才发送触发信号
	if needFlush {
		select {
		case bc.freshTriggerChan <- struct{}{}: // 非阻塞发送
		default:
			// 通道已满说明已有待处理信号
		}
	}
}

func (bc *BatchCache[T]) QueueLength() int {
	return int(atomic.LoadInt64(&bc.metrics.queueLength))
}

func (bc *BatchCache[T]) ActiveFlushes() int {
	return int(atomic.LoadInt64(&bc.metrics.activeFlush))
}

func (bc *BatchCache[T]) run() {
	defer bc.wg.Done()
	defer bc.flushTimer.Stop()

	bc.flushTimer.Reset(bc.flushInterval)

	for {
		select {
		case <-bc.stopChan:
			bc.safeFlush()
			return

		case <-bc.freshTriggerChan:
			// 只有收到信号时才处理，确保不重复
			if bc.flushing.CompareAndSwap(false, true) {
				bc.safeFlushWithReset()
				bc.flushing.Store(false) // 直接同步释放标记
			}

		case <-bc.flushTimer.C:
			if bc.flushing.CompareAndSwap(false, true) {
				bc.safeFlushWithReset()
				bc.flushing.Store(false) // 直接同步释放标记
			} else {
				// 如果已经在刷新，只需重置定时器
				bc.flushTimer.Reset(bc.flushInterval)
			}
		}
	}
}

func (bc *BatchCache[T]) safeFlush() {
	bc.mutex.Lock()
	if len(bc.buffer) == 0 {
		bc.mutex.Unlock()
		return
	}

	// 交换缓冲区
	batch := bc.buffer
	bc.buffer = make([]T, 0, bc.maxItems)
	atomic.StoreInt64(&bc.metrics.queueLength, 0)
	bc.mutex.Unlock()

	if len(batch) > 0 {
		bc.executeProcessor(batch)
	}
}

func (bc *BatchCache[T]) safeFlushWithReset() {
	bc.safeFlush()
	bc.flushTimer.Reset(bc.flushInterval)
}

func (bc *BatchCache[T]) executeProcessor(batch []T) {
	bc.pendingWg.Add(1)
	atomic.AddInt64(&bc.metrics.activeFlush, 1)

	go func() {
		defer func() {
			atomic.AddInt64(&bc.metrics.activeFlush, -1)
			bc.pendingWg.Done()
		}()

		// 获取信号量(如果有限制)
		if bc.maxConcurrent > 0 {
			<-bc.semaphore
			defer func() { bc.semaphore <- struct{}{} }()
		}

		bc.processor(batch)
	}()
}
