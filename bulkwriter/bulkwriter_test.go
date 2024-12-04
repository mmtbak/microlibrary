// Package bulkwriter
package bulkwriter

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"gopkg.in/go-playground/assert.v1"
)

func TestNewBulkWriter(t *testing.T) {

	wf := func(t []int) {
		// simulate  long time task
		time.Sleep(3 * time.Second)
		fmt.Println("输入长度：", len(t))
	}
	writer := NewBulkWriter[int](10000, 2*time.Second,
		wf,
	)
	assert.Equal(t, writer.msglimit, 10000)
	assert.Equal(t, writer.tick, 2*time.Second)
}

// TestAppend 测试Append
func BenchmarkBulkWriterPerfermance(*testing.B) {
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

	wf := func(t []int) {
		mu.Lock()
		calcNum += len(t)
		mu.Unlock()
	}

	writer := NewBulkWriter(bluklimit, 2*time.Second, wf).WithWriterLimit(100)

	// monitor calcNum
	go func() {
		for {
			time.Sleep(1 * time.Second)
			fmt.Println(time.Now(), ":  calcNUm: ", calcNum)
		}
	}()

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

	wg.Wait()
	// 等待writer干完
	time.Sleep(4 * time.Second)
	fmt.Println("总量:", calcNum)
}
