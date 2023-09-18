package client

import (
	"gdfs/internal/types"
	"log"
	"sync"
	"time"
)

type leaseBuffer struct {
	sync.RWMutex
	buffer map[types.ChunkHandle]*types.LeaseInfo
	tick   time.Duration
	cli    *Client
	roleCh chan struct{}
}

// 管理关于handle的租约信息
func newLeaseBuffer(cli *Client, tick time.Duration) *leaseBuffer {
	buf := &leaseBuffer{
		buffer: make(map[types.ChunkHandle]*types.LeaseInfo),
		tick:   tick,
		cli:    cli,
		roleCh: make(chan struct{}, 0),
	}

	// clean task
	go func() {
		ticker := time.NewTicker(tick)
		for {
			select {
			case <-ticker.C:
				now := time.Now()
				buf.Lock()
				for id, item := range buf.buffer {
					if item.Exipre.Before(now) {
						delete(buf.buffer, id)
					}
				}
				buf.Unlock()
			case <-buf.roleCh:
				log.Println("detect meta-server change,clear all stale lease info!")
				buf.clearAllLease()
			}
		}
	}()

	return buf
}

func (buf *leaseBuffer) Get(handle types.ChunkHandle) (*types.LeaseInfo, error) {
	buf.Lock()
	defer buf.Unlock()
	lease, ok := buf.buffer[handle]

	if !ok { // ask master to send one
		var l types.GetPrimaryAndSecondariesReply
		arg := types.GetPrimaryAndSecondariesArg{
			ClientIdentity: types.ClientIdentity{},
			Handle:         handle,
		}
		err := buf.cli.do("Master.RPCGetPrimaryAndSecondaries", &arg, &l)
		if err != nil {
			return nil, err
		}

		lease = &types.LeaseInfo{
			Primary: l.Primary,
			Backups: l.Secondaries,
			Exipre:  l.Expire,
		}
		buf.buffer[handle] = lease
		return lease, nil
	}
	// extend lease (it is the work of chunk server)
	/*
	   go func() {
	       var r types.ExtendLeaseReply
	       util.Call(buf.master, "Master.RPCExtendLease", types.ExtendLeaseArg{handle, lease.Primary}, &r)
	       lease.Expire = r.Expire
	   }()
	*/
	return lease, nil
}

// call by master change
func (l *leaseBuffer) clearAllLease() {
	l.Lock()

	for handle := range l.buffer {
		delete(l.buffer, handle)
	}
	l.Unlock()

}
