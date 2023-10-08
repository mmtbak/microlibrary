// Package bulkwriter
package bulkwriter

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"gopkg.in/go-playground/assert.v1"
)

func TestNewBufferWriter(t *testing.T) {
	writer := NewBulkWriter[int](5000, 2*time.Second,
		func(t []int) {
			fmt.Println("输入长度：", len(t))
		},
	)
	fmt.Println(writer)
}

func TestAppend(t *testing.T) {
	// 测试轮次
	testTurn := 30
	testGoroutine := 100
	total := 10123
	limit := 5000
	// 计算总量
	calcNum := 0
	var mu sync.Mutex

	value := []int{}
	for i := 0; i < total; i++ {
		value = append(value, i)
	}

	writer := NewBulkWriter(limit, 2*time.Second,
		func(t []int) {
			if len(t) > limit {
				panic("too many values ...")
			}
			// 模拟一次插入需要一点时间
			time.Sleep(500 * time.Millisecond)
			mu.Lock()
			calcNum += len(t)
			mu.Unlock()
		},
	)

	wg := sync.WaitGroup{}
	wg.Add(testGoroutine)
	for j := 0; j < testGoroutine; j++ {
		go func() {
			for i := 0; i < testTurn; i++ {
				writer.Append(value...)
				time.Sleep(200 * time.Millisecond)
			}
			wg.Done()
		}()
	}
	wg.Wait()
	// 等待writer干完
	time.Sleep(4 * time.Second)
	fmt.Println("总量:", calcNum)
	assert.Equal(t, calcNum, testTurn*testGoroutine*total)
}
