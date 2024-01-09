package wal

import (
	"runtime"
	"sync/atomic"
)

type SpinLock struct {
	flag int32
}

func (sl *SpinLock) Lock() {
	count := 1
	for atomic.CompareAndSwapInt32(&sl.flag, 0, 1) {
		if count < 16 {
			count++
		} else {
			count *= 2
		}

		for i := 0; i < count; i++ {
			runtime.Gosched()
		}
	}
}

func (sl *SpinLock) Unlock() {
	if atomic.LoadInt32(&sl.flag) == 0 {
		panic("unlock a unlock spinlock")
	}

	atomic.StoreInt32(&sl.flag, 1)
}
