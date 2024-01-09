package client

import (
	"gdfs/types"
	"log"
	"sync"
	"time"
)

type leaseBuffer struct {
	sync.RWMutex
	buffer   map[types.ChunkHandle]*types.LeaseInfo
	endpoint map[types.ChunkHandle][]types.EndpointInfo
	tick     time.Duration
	cli      *Client
	roleCh   chan struct{}
}

// 管理关于handle的租约信息
func newLeaseBuffer(cli *Client, tick time.Duration) *leaseBuffer {
	buf := &leaseBuffer{
		buffer:   make(map[types.ChunkHandle]*types.LeaseInfo),
		tick:     tick,
		cli:      cli,
		endpoint: make(map[types.ChunkHandle][]types.EndpointInfo),
		roleCh:   make(chan struct{}, 0),
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

func (buf *leaseBuffer) GetEndpoint(handle types.ChunkHandle) ([]types.EndpointInfo, error) {
	buf.Lock()
	defer buf.Unlock()

	ends, ok := buf.endpoint[handle]

	if !ok {
		var l types.GetReplicasReply
		arg := types.GetReplicasArg{
			Handle: handle,
		}
		err := buf.cli.do("Master.RPCGetReplicas", &arg, &l)
		if err != nil {
			return nil, err
		}

		buf.endpoint[handle] = l.Locations
	}

	return ends, nil
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
			Primary: l.Primary.Addr,
			Exipre:  l.Expire,
		}
		buf.endpoint[handle] = append(buf.endpoint[handle], types.EndpointInfo{
			Addr:     l.Primary.Addr,
			Property: l.Primary.Property,
		})
		for idx := range l.Secondaries {
			lease.Backups = append(lease.Backups, l.Secondaries[idx].Addr)
			buf.endpoint[handle] = append(buf.endpoint[handle], types.EndpointInfo{
				Addr:     l.Secondaries[idx].Addr,
				Property: l.Secondaries[idx].Property,
			})
		}

		buf.buffer[handle] = lease
		return lease, nil
	}
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
