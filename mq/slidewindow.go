package mq

import (
	"errors"
	"sync"
)

// SlideWindow sliding window for kafka message consumer commit
type SlideWindow[T any] struct {
	// window size
	WindowSize int
	// window data
	Window []T
	mutex  *sync.Mutex
	cond   *sync.Cond
}

func NewSlideWindow[T any](size int) (*SlideWindow[T], error) {

	if size <= 0 {
		return nil, errors.New("size must be greater than 0")
	}

	mutex := &sync.Mutex{}
	return &SlideWindow[T]{
		WindowSize: size,
		Window:     make([]T, 0, size),
		mutex:      mutex,
		cond:       sync.NewCond(mutex),
	}, nil
}

// Add add data to window, if window is full, it will block until window has space
func (w *SlideWindow[T]) Add(data T) {

	w.mutex.Lock()
	defer w.mutex.Unlock()
	for len(w.Window) >= w.WindowSize {
		w.cond.Wait()
	}
	w.Window = append(w.Window, data)
}

// SlideWihFunc slide window with function, if function return true, it will slide to next, until function return false
// f is the function to slide window, if function return true, it will slide to next, until function return false
// return the last data that can slide to window, and the total number of data that can slide to window
func (w *SlideWindow[T]) SlideWihFunc(f func(T) bool) (T, int) {

	var data T
	var count int = 0
	// lock operation
	w.mutex.Lock()
	defer w.mutex.Unlock()

	for idx := range w.Window {
		if f(w.Window[idx]) {
			data = w.Window[idx]
			count++
			continue
		}
		break
	}
	if count > 0 {
		w.Window = w.Window[count:]
		w.cond.Broadcast()
	}
	return data, count
}

type SlideWindowsMap[T any] struct {
	windows map[string]*SlideWindow[T]
	mutex   *sync.RWMutex
}

func NewSlideWindows[T any](size int) (*SlideWindowsMap[T], error) {
	windows := make(map[string]*SlideWindow[T])
	mutex := &sync.RWMutex{}
	return &SlideWindowsMap[T]{
		windows: windows,
		mutex:   mutex,
	}, nil
}

func (s *SlideWindowsMap[T]) Add(key string, data T) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.windows[key].Add(data)
}

func (s *SlideWindowsMap[T]) SlideWihFunc(key string, f func(T) bool) (T, int) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.windows[key].SlideWihFunc(f)
}
