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
	writer := NewBulkWriter[int](10000, 2*time.Second,
		func(t []int) {
			// simulate  long time task
			time.Sleep(3 * time.Second)
			fmt.Println("输入长度：", len(t))
		},
	)
	fmt.Println(writer)
}

func TestWriter(t *testing.T) {
	// 测试轮次
	testTurn := 3000
	numofGoroutine := 100
	total := 10123
	datalimit := 1000
	bluklimit := 10 * datalimit
	// 计算总量
	calcNum := 0
	var mu sync.Mutex

	value := []int{}
	for i := 0; i < total; i++ {
		value = append(value, i)
	}

	writer := NewBulkWriter(bluklimit, 2*time.Second,
		func(t []int) {
			fmt.Println("in writer func , datalen :", len(t))
			if len(t) > bluklimit {
				panic("too many values ...")
			}
			// 模拟一次插入需要一点时间
			time.Sleep(1 * time.Second)
			mu.Lock()
			calcNum += len(t)
			mu.Unlock()
		},
	).WithWriterLimit(100)

	wg := sync.WaitGroup{}
	wg.Add(numofGoroutine)
	for j := 0; j < numofGoroutine; j++ {
		fmt.Println("start goroutine:", j)
		go func() {
			for i := 0; i < testTurn; i++ {
				writer.Append(value...)
				time.Sleep(200 * time.Millisecond)
			}
			wg.Done()
		}()
	}

	go func() {
		for {
			time.Sleep(1 * time.Second)
			fmt.Println(time.Now(), ":  calcNUm: ", calcNum)
		}
	}()
	wg.Wait()
	// 等待writer干完
	time.Sleep(4 * time.Second)
	fmt.Println("总量:", calcNum)
	assert.Equal(t, calcNum, testTurn*numofGoroutine*total)
}
