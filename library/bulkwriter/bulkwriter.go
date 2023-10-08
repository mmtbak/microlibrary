// Package bulkwriter
// 批量处理方法，用于累计一段时间或者累计到一定量的数据都批量 插入或者输出
package bulkwriter

import (
	"sync"
	"sync/atomic"
	"time"
)

// BulkWriter 批量处理器
type BulkWriter[T any] struct {
	mu         sync.Mutex
	buffer     []T
	limit      int
	tick       time.Duration
	ticker     *time.Ticker
	CBFunc     func([]T)
	bufferFull chan interface{}
	// 条件变量
	cond *sync.Cond
	// 用于监控
	queueSize               int64
	queueSizePeriod         int64
	itemSize                int64
	itemSizePeriod          int64
	waitingWorkerSize       int64
	waitingWorkerSizePeriod int64
}

// NewBulkWriter 创建一个批量处理缓存器， 当缓存器缓存的数量达到一定条件后，触发一个批次处理函数。清理缓存数据
// @limit 缓存数据数量上限
// @tick 缓存时间上限
// @buckfunc 批量处理函数
func NewBulkWriter[T any](limit int, tick time.Duration, bulkfunc func([]T)) *BulkWriter[T] {
	writer := BulkWriter[T]{
		limit:      limit,
		buffer:     make([]T, 0, limit),
		tick:       tick,
		ticker:     time.NewTicker(tick),
		CBFunc:     bulkfunc,
		bufferFull: make(chan interface{}),
		// 条件变量
		cond: sync.NewCond(&sync.Mutex{}),
	}

	// 定时函数
	//  buffer满或者 时间到了，清理缓存
	go func() {
		for {
			select {
			// buffer满了
			case <-writer.bufferFull:
				writer.Flush()
			// 定时转存
			case <-writer.ticker.C:
				writer.Flush()
			}
		}
	}()

	// // 监控函数
	// go func() {
	// 	for writer.logger != nil {
	// 		time.Sleep(10 * time.Second)
	// 		msg := fmt.Sprintf(`Type name: %v, buffer size: %v, queue size: %v, item size: %v, waiting worker size: %v,
	// 		queue size period: %v, item size period: %v, waiting worker size period: %v.\n`,
	// 			reflect.TypeOf(new(T)).Elem().Name(), len(writer.buffer), writer.queueSize, writer.itemSize,
	// 			writer.waitingWorkerSize,
	// 			writer.queueSizePeriod, writer.itemSizePeriod, writer.waitingWorkerSizePeriod)
	// 		writer.logger.Info(msg)
	// 		writer.resetPeriod()
	// 	}
	// }()

	return &writer
}

// Flush 将已有buffer全部写回数据库，并发消息给Append函数：buffer已经刷清空
func (writer *BulkWriter[T]) Flush() {
	writer.mu.Lock()
	defer writer.mu.Unlock()

	// 清空满的信号, 如果有。
	select {
	case <-writer.bufferFull:
	default:
	}

	// 调用CBFunc
	if len(writer.buffer) > 0 {
		// 每次最多只读limit个，然后循环
		for len(writer.buffer) > 0 {
			readLen := min(len(writer.buffer), writer.limit)

			go func(buffer []T) {
				writer.addInsertQueueAndItemsSize(len(buffer))
				writer.CBFunc(buffer)
				writer.subInsertQueueAndItemsSize(len(buffer))
			}(writer.buffer[:readLen])

			writer.buffer = writer.buffer[readLen:]
		}
		// 新建buffer
		writer.buffer = make([]T, 0, writer.limit)
		writer.brocast()
	}
	// 重置ticker
	writer.ticker.Reset(writer.tick)
}

// Append Append
func (writer *BulkWriter[T]) Append(values ...T) {
	if len(values) == 0 {
		return
	}

	// 等待append的worker数量
	writer.addwaitingWorkerSize()
	defer writer.subwaitingWorkerSize()

	writer.mu.Lock()
	// 先看看buffer是不是满了
	for len(writer.buffer) >= writer.limit {
		// 如果满了，释放锁，给Flush发消息，然后等到条件变量释放
		writer.mu.Unlock()
		writer.bufferFull <- nil
		writer.wait()
		// 等待结束，缓冲区已经清空，重新尝试获取锁给缓冲区放数据
		writer.mu.Lock()
	}
	writer.buffer = append(writer.buffer, values...)
	writer.mu.Unlock()
}

func (writer *BulkWriter[T]) wait() {
	writer.cond.L.Lock()
	for len(writer.buffer) > writer.limit {
		writer.cond.Wait()
	}
	writer.cond.L.Unlock()
}

func (writer *BulkWriter[T]) brocast() {
	writer.cond.Broadcast()
}

// addInsertQueueAndItemsSize 加一些监控值
func (writer *BulkWriter[T]) addInsertQueueAndItemsSize(itemSize int) {
	atomic.AddInt64(&writer.queueSize, 1)
	atomic.AddInt64(&writer.queueSizePeriod, 1)
	atomic.AddInt64(&writer.itemSize, int64(itemSize))
	atomic.AddInt64(&writer.itemSizePeriod, int64(itemSize))
}

// subInsertQueueAndItemsSize 减一些监控值
func (writer *BulkWriter[T]) subInsertQueueAndItemsSize(itemSize int) {
	atomic.AddInt64(&writer.queueSize, -1)
	atomic.AddInt64(&writer.itemSize, -int64(itemSize))
}

// addwaitingWorkerSize 加一些监控值
func (writer *BulkWriter[T]) addwaitingWorkerSize() {
	atomic.AddInt64(&writer.waitingWorkerSize, 1)
	atomic.AddInt64(&writer.waitingWorkerSizePeriod, 1)
}

// subwaitingWorkerSize 减一些监控值
func (writer *BulkWriter[T]) subwaitingWorkerSize() {
	atomic.AddInt64(&writer.waitingWorkerSize, -1)
}

// resetPeriod 重置reset
func (writer *BulkWriter[T]) resetPeriod() {
	atomic.AddInt64(&writer.queueSizePeriod, -writer.queueSizePeriod)
	atomic.AddInt64(&writer.itemSizePeriod, -writer.itemSizePeriod)
	atomic.AddInt64(&writer.waitingWorkerSizePeriod, -writer.waitingWorkerSizePeriod)
}

// min min is the minimum value
func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}
