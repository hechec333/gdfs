package raft

import (
	"gdfs/internal/common"
	"sync"
	"sync/atomic"
	"time"
)

// thread safe only leader can KeepLease
// client command can read OnLease

// pp2
// one worst case when the parlition happen and at the mean time has two leader exists.
// that means the leader lease still can keep by the stale leader not in the marjority group.

// a leader can hold lease at most election-timeout when there have no split brain problem
// one turn successful heartbeat can keep lease anothor election-timeout

// this is one way optism,the lease only can tell current leader is possible only alone leader except
// the situation @pp2, this isn't enough to be only judgement to implement the local read safety

// eg accroding current implements of Raft. along with the begin duration of a new leader,its commitindex
// may not always to be the newest,because it won't try to commit elder term leader

// the open interface should include the emrgency call imideiately sync new heartbeat to make sure

// to fix!! the below module should implement as a basic funtion in raft module will merge to raft.go
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
	common.LTrace("<DEBUG> start %v now %v expire %v", ll.start, time.Now(), ll.exipre)
	return time.Now().After(ll.start) && time.Now().Before(ll.exipre)
}

// leader call heartbeat when follower responce
// only first responcer can set startpoint
func (ll *LeaderLease) startpoint() {
	if atomic.CompareAndSwapInt32(&ll.flag, 0, 1) {
		ll.Lock()
		defer ll.Unlock()
		ll.start = time.Now()
		common.LTrace("<Raft> new start point at %v", ll.start)
	}
}

// call when more than half members success responce leader heatbeat
func (ll *LeaderLease) keepLease(d time.Duration) {
	atomic.StoreInt32(&ll.flag, 0)
	ll.Lock()
	defer ll.Unlock()
	ll.RefreshCount++
	ll.exipre = ll.start.Add(d)
	common.LTrace("<Raft> new lease deadline %v", ll.exipre)
}
