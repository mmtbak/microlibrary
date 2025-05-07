package mq

import (
	"log/slog"
	"math/rand"
	"testing"
	"time"

	"github.com/panjf2000/ants/v2"
)

type MyData struct {
	Name        string
	IsProcessed bool
}

var DataList = []*MyData{
	&MyData{Name: "1", IsProcessed: false},
	&MyData{Name: "2", IsProcessed: false},
}

func CheckProcessed(data *MyData) bool {
	return data.IsProcessed
}

func MessageProcess(i interface{}) {

	data, ok := i.(*MyData)
	if !ok {
		slog.Error("data type error")
		return
	}

	rands := int(rand.Float32() * 10)
	d := time.Duration(rands) * time.Second
	slog.Info("start process data", "name", data.Name, "duration", d)

	time.Sleep(d)
	slog.Info("finish process data", "name", data.Name)
	data.IsProcessed = true

}

func TestSlideWindow(t *testing.T) {

	var size int = 3
	workpool, err := ants.NewPoolWithFunc(size, MessageProcess, ants.WithNonblocking(false))
	if err != nil {
		t.Error(err)
		return
	}
	sw, err := NewSlideWindow[*MyData](size)
	if err != nil {
		t.Error(err)
		return
	}

	// mock commit data with ticker
	go func() {
		for {
			ticker := time.NewTicker(1 * time.Second)
			select {
			case <-ticker.C:
				last, count := sw.SlideWihFunc(CheckProcessed)
				if count != 0 {
					slog.Info("last data", "name", last.Name, "count", count)
				}
			}
		}
	}()

	// mock add data to window
	for idx := range DataList {
		sw.Add(DataList[idx])
		slog.Info("add data", "name", DataList[idx].Name)
		workpool.Invoke(DataList[idx])
		// mock data add
	}
}
