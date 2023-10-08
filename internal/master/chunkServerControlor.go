package master

import (
	"fmt"
	"gdfs/internal/common"
	"gdfs/internal/types"
	"log"
	"math/rand"
	"sync"
	"time"
)

type ChunkServerInfo struct {
	heartbeat time.Time
	chunks    []types.ChunkHandle
	garbage   []types.ChunkHandle
}
type ChunkServerControlor struct {
	sync.RWMutex
	servers map[types.Addr]*ChunkServerInfo
}

func NewChunkServerControlor() *ChunkServerControlor {
	csc := &ChunkServerControlor{}
	csc.ClearAllVoliateStateAndRebuild()
	return csc
}

func (csi *ChunkServerInfo) HasChunk(handle types.ChunkHandle) bool {
	for _, v := range csi.chunks {
		if v == handle {
			return true
		}
	}
	return false
}
func (csc *ChunkServerControlor) ClearAllVoliateStateAndRebuild() {
	csc.servers = make(map[types.Addr]*ChunkServerInfo)
}
func (csc *ChunkServerControlor) HeartBeat(server types.Addr, reply *types.HeartbeatReply) bool {
	csc.Lock()
	defer csc.Unlock()

	cs, ok := csc.servers[server]
	if !ok {
		//log
		// 发现server
		csc.servers[server] = &ChunkServerInfo{
			heartbeat: time.Now(),
			chunks:    make([]types.ChunkHandle, 0),
			garbage:   make([]types.ChunkHandle, 0),
		}
		return true
	} else {
		// 已注册的server发送垃圾回收
		reply.Garbage = cs.garbage
		cs.garbage = make([]types.ChunkHandle, 0)
		cs.heartbeat = time.Now()
		return false
	}
}

func (csc *ChunkServerControlor) AddChunk(servers []types.Addr, handle types.ChunkHandle) {
	csc.Lock()
	defer csc.Unlock()

	for _, addr := range servers {
		cs, ok := csc.servers[addr]
		if ok && !cs.HasChunk(handle) {
			cs.chunks = append(cs.chunks, handle)
		} else {
			log.Printf("%v server removed", addr)
		}
	}
}

func (csc *ChunkServerControlor) AddGarbage(server types.Addr, handle types.ChunkHandle) {
	csc.Lock()
	defer csc.Unlock()

	if cs, ok := csc.servers[server]; ok {
		cs.garbage = append(cs.garbage, handle)
	}
}

// 在分片不足时，调度一个已知分片到另外一个没有分片的服务器
func (csc *ChunkServerControlor) CopyChunk(handle types.ChunkHandle) (from, to types.Addr, err error) {
	owns := []types.Addr{}
	notowns := []types.Addr{}

	csc.RLock()
	for k, v := range csc.servers {
		if v.HasChunk(handle) {
			owns = append(owns, k)
		} else {
			notowns = append(notowns, k)
		}
	}
	csc.RUnlock()
	if len(notowns) < 1 {
		return "", "", fmt.Errorf("not enough server to schedule")
	}
	//调度策略
	k := rand.Intn(len(owns))
	i := rand.Intn(len(notowns))
	common.LInfo("copy server %v to %v", owns[k], notowns[i])
	return owns[k], notowns[i], nil
}

// 获取当前活跃可以派发分片的cs
func (csc *ChunkServerControlor) ScheduleActiveServers(num int) ([]types.Addr, error) {
	all := []types.Addr{}
	csc.RLock()
	for s := range csc.servers {
		all = append(all, s)
	}
	csc.RUnlock()
	if num > len(all) || num < 1 {
		return nil, fmt.Errorf("invalid numbers of servers")
	}
	seq := rand.Perm(len(all))[:num]
	selects := []types.Addr{}
	for _, v := range seq {
		selects = append(selects, all[v])
	}
	common.LInfo("new sharding to %v", selects)
	return selects, nil
}

func (csc *ChunkServerControlor) GetDeadServers() []types.Addr {
	csc.RLock()
	defer csc.RUnlock()
	now := time.Now()
	dead := []types.Addr{}
	for k, server := range csc.servers {
		if server.heartbeat.Add(common.HeartBeatDuration).Before(now) {
			dead = append(dead, k)
		}
	}
	return dead
}

func (csc *ChunkServerControlor) RemoveServer(server types.Addr) []types.ChunkHandle {
	csc.Lock()
	defer csc.Unlock()
	common.LInfo("Call RemoveServer to %v", server)
	cs, ok := csc.servers[server]
	if !ok {
		return nil
	}

	chunks := cs.chunks

	delete(csc.servers, server)
	return chunks
}
