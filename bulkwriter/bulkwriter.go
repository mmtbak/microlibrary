// Package bulkwriter
// bulkwriter 是一个数据缓存器，当缓存器缓存的数量达到限制或者定时达到限制后，触发一次批处理，清理缓存数据. 再重置定时器与limit计数器
// 针对场景类似ElasticSearch/Clickhouse等AnalysisDB，大量单次写入时会触发写限制而造成业务异常，需要一次批量写入缓解写入负载。 bulkwriter将单个数据合并成一个大集合数据，再触发一次性出行
package bulkwriter

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/panjf2000/ants/v2"
)

// BulkWriter 批量处理器.
type BulkWriter[T any] struct {
	mu          sync.Mutex
	buffer      []T           // buffer 数据缓存
	msglimit    int           // 批处理消息数量上限
	tick        time.Duration // 批处理定时时间长度
	WriteFunc   func([]T)     // 回调批量处理函数 ，批量处理器
	writerlimit int           // 最大writer数量
	ticker      *time.Ticker
	bufferFull  chan interface{} // buffer满的信号量
	// 条件变量
	cond *sync.Cond

	// 执行池
	pool *ants.PoolWithFunc

	// 用于监控
	queueSize               int64
	queueSizePeriod         int64
	itemSize                int64
	itemSizePeriod          int64
	waitingWorkerSize       int64
	waitingWorkerSizePeriod int64
}

// NewBulkWriter 创建一个批量处理缓存器， 当缓存器缓存的消息数量达到 limit 或者时间达到tick之后，触发一个批次处理函数writerfunc,清理缓存数据.
// @limit 批处理消息数量上限
// @tick 批处理缓存定时时间长度
// @writerfunc 批量处理函数.
func NewBulkWriter[T any](limit int, tick time.Duration, writefunc func([]T)) *BulkWriter[T] {
	writer := BulkWriter[T]{
		msglimit:   limit,
		buffer:     make([]T, 0, limit),
		tick:       tick,
		ticker:     time.NewTicker(tick),
		WriteFunc:  writefunc,
		bufferFull: make(chan interface{}),
		// 条件变量
		cond: sync.NewCond(&sync.Mutex{}),
	}

	// 默认只有一一个执行
	writer.pool, _ = ants.NewPoolWithFunc(1, writer.write, ants.WithNonblocking(false))

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

	return &writer
}

// WithWriterLimit set writer limit.
func (w *BulkWriter[T]) WithWriterLimit(limit int) *BulkWriter[T] {
	w.writerlimit = limit
	w.pool, _ = ants.NewPoolWithFunc(limit, w.write, ants.WithNonblocking(false))
	return w
}

// Flush 将已有buffer全部写回数据库，并发消息给Append函数：buffer已经刷清空.
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
			readLen := min(len(writer.buffer), writer.msglimit)
			writer.pool.Invoke(writer.buffer[:readLen])

			writer.buffer = writer.buffer[readLen:]
		}
		// 新建buffer
		writer.buffer = make([]T, 0, writer.msglimit)
		writer.brocast()
	}
	// 重置ticker
	writer.ticker.Reset(writer.tick)
}

// write callback processfunc.
func (w *BulkWriter[T]) write(i interface{}) {
	data := i.([]T)
	w.addInsertQueueAndItemsSize(len(data))
	w.WriteFunc(data)
	w.subInsertQueueAndItemsSize(len(data))
}

// Append Append.
func (writer *BulkWriter[T]) Append(values ...T) {
	if len(values) == 0 {
		return
	}

	// 等待append的worker数量
	writer.addwaitingWorkerSize()
	defer writer.subwaitingWorkerSize()

	writer.mu.Lock()
	// 先看看buffer是不是满了
	for len(writer.buffer) >= writer.msglimit {
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
	for len(writer.buffer) > writer.msglimit {
		writer.cond.Wait()
	}
	writer.cond.L.Unlock()
}

func (writer *BulkWriter[T]) brocast() {
	writer.cond.Broadcast()
}

// addInsertQueueAndItemsSize 加一些监控值.
func (writer *BulkWriter[T]) addInsertQueueAndItemsSize(itemSize int) {
	atomic.AddInt64(&writer.queueSize, 1)
	atomic.AddInt64(&writer.queueSizePeriod, 1)
	atomic.AddInt64(&writer.itemSize, int64(itemSize))
	atomic.AddInt64(&writer.itemSizePeriod, int64(itemSize))
}

// subInsertQueueAndItemsSize 减一些监控值.
func (writer *BulkWriter[T]) subInsertQueueAndItemsSize(itemSize int) {
	atomic.AddInt64(&writer.queueSize, -1)
	atomic.AddInt64(&writer.itemSize, -int64(itemSize))
}

// addwaitingWorkerSize 加一些监控值.
func (writer *BulkWriter[T]) addwaitingWorkerSize() {
	atomic.AddInt64(&writer.waitingWorkerSize, 1)
	atomic.AddInt64(&writer.waitingWorkerSizePeriod, 1)
}

// subwaitingWorkerSize 减一些监控值.
func (writer *BulkWriter[T]) subwaitingWorkerSize() {
	atomic.AddInt64(&writer.waitingWorkerSize, -1)
}

// resetPeriod 重置reset.
func (writer *BulkWriter[T]) resetPeriod() {
	atomic.AddInt64(&writer.queueSizePeriod, -writer.queueSizePeriod)
	atomic.AddInt64(&writer.itemSizePeriod, -writer.itemSizePeriod)
	atomic.AddInt64(&writer.waitingWorkerSizePeriod, -writer.waitingWorkerSizePeriod)
}

// min min is the minimum value.
func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}
