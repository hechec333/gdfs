package raft

import (
	"sync"
	"sync/atomic"
	"time"
)

// thread safe only leader can KeepLease
// client command can read OnLease

// a leader can hold lease at most election-timeout when there have no split brain problem
// one turn successful heartbeat can keep lease anothor election-timeout
type LeaderLease struct {
	sync.RWMutex
	exipre       time.Time
	start        time.Time
	flag         int32
	RefreshCount int64
}

// check lease
func (ll *LeaderLease) OnLease() bool {
	ll.RLock()
	defer ll.RUnlock()
	return time.Now().After(ll.start) && time.Now().Before(ll.exipre)
}

// leader call heartbeat when follower responce
// only first responcer can set startpoint
func (ll *LeaderLease) startpoint() {

	if atomic.CompareAndSwapInt32(&ll.flag, 0, 1) {
		ll.Lock()
		defer ll.Unlock()
		ll.start = time.Now()
	}
}

// call when more than half members success responce leader heatbeat
func (ll *LeaderLease) keepLease(d time.Duration) {
	atomic.StoreInt32(&ll.flag, 0)
	ll.Lock()
	defer ll.Unlock()
	ll.RefreshCount++
	ll.exipre = ll.start.Add(d)
}
