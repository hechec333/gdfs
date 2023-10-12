package master

import (
	"context"
	"fmt"
	"gdfs/internal/common"
	"gdfs/internal/common/rpc"
	"gdfs/internal/types"
	"gdfs/internal/wal"
	"log"
	"math/rand"
	"sort"
	"sync"
	"time"
)

type Lease struct {
	Location []types.Addr
	Primary  types.Addr
	Expire   time.Time
}

type ChunkInfo struct {
	sync.RWMutex
	//replica
	Primary  types.Addr   //主
	Replicas []types.Addr //从
	CheckSum int64        //检验和

	//lease
	Version int //每次写入更新，有cs推送到master
	Expire  time.Time

	//meta
	Path types.Path
}

type FileInfo struct {
	sync.RWMutex
	RefCount int //引用计数
	//Handles  []types.ChunkHandle
	Handles map[int]types.ChunkHandle // 考虑到chunk全部丢失的情况
}

func (fi *FileInfo) HasChunk(vv types.ChunkHandle) int {
	for i, v := range fi.Handles {
		if v == vv {
			return i
		}
	}
	return -1
}

type ChunkControlor struct {
	sync.RWMutex
	Chunk           map[types.ChunkHandle]*ChunkInfo
	Files           map[types.Path]*FileInfo
	replicaNeedList []types.ChunkHandle
	//ChunkHandleSeed types.ChunkHandle

	OperationLog []types.ChunkLogImpl // clear after call
}

func NewChunkControlor() *ChunkControlor {
	return &ChunkControlor{
		Chunk:           make(map[types.ChunkHandle]*ChunkInfo),
		Files:           make(map[types.Path]*FileInfo),
		replicaNeedList: make([]types.ChunkHandle, 0),
	}
}

// persite
func (cc *ChunkControlor) Serialize() (data types.PersiteChunkControlor) {
	for _, v := range cc.Chunk {
		m := make(map[int]types.PersiteChunkInfo)
		for i, vv := range cc.Files[v.Path].Handles {
			m[i] = types.PersiteChunkInfo{
				Version:     cc.Chunk[vv].Version,
				CheckSum:    int(cc.Chunk[vv].CheckSum),
				ChunkHandle: vv,
			}
		}
		data.Files = append(data.Files, types.PersiteFileInfo{
			Path:     v.Path,
			RefCount: cc.Files[v.Path].RefCount,
			Chunk:    m,
		})
	}
	return
}

func (cc *ChunkControlor) Desrialize(data types.PersiteChunkControlor) {
	files := make(map[types.Path]*FileInfo)
	chunks := make(map[types.ChunkHandle]*ChunkInfo)
	for _, v := range data.Files {
		maps := make(map[int]types.ChunkHandle)
		for idx, info := range v.Chunk {
			maps[idx] = info.ChunkHandle
			chunks[info.ChunkHandle] = &ChunkInfo{
				Version:  info.Version,
				CheckSum: int64(info.CheckSum),
				Expire:   time.Now(),
			}
		}
		files[v.Path] = &FileInfo{
			RefCount: v.RefCount,
			Handles:  maps,
		}
	}

	cc.Lock()
	defer cc.Unlock()
	cc.Files = files
	cc.Chunk = chunks
}

func (cc *ChunkControlor) ClearVolitateStateAndRebuild() {
	cc.Lock()
	defer cc.Unlock()

	cc.replicaNeedList = make([]types.ChunkHandle, 0)
	cc.OperationLog = make([]types.ChunkLogImpl, 0)
	for i, v := range cc.Chunk {
		if len(v.Replicas) != 0 {
			cc.Chunk[i].Replicas = make([]types.Addr, 0)
		}
		cc.Chunk[i].CheckSum = 0
		cc.Chunk[i].Primary = types.Addr("")
	}
}

func (cc *ChunkControlor) SavePersiteState() types.PersiteChunkControlor {
	return cc.Serialize()
}
func (cc *ChunkControlor) ReadPersiteState(meta types.PersiteChunkControlor) error {
	if len(meta.Files) == 0 {
		return fmt.Errorf("empty persite state")
	}
	cc.ReadPersiteState(meta)
	return nil
}

// wal
func (cc *ChunkControlor) MustCreateChunk(do *wal.LogOpLet, f *FileInfo, path types.Path) (types.ChunkHandle, error) {
	f.Lock()
	defer f.Unlock()
	handle := types.ChunkHandle(common.GetChunkHandleId())
	f.Handles[len(f.Handles)] = handle
	log := types.ChunkLogImpl{
		CommandType: types.CommandCreate,
		Path:        path,
		Chunk: types.PersiteChunkInfo{
			Version:     1,
			ChunkHandle: handle,
		},
	}
	ctx, h := context.WithTimeout(context.TODO(), 1*time.Second)
	defer h()
	return handle, do.ChunkStartCtx(ctx, log)
}

func (cc *ChunkControlor) applyCreateChunk(path types.Path, meta types.PersiteChunkInfo) error {
	cc.Lock()
	defer cc.Unlock()
	file, ok := cc.Files[path]
	if !ok {
		cc.Files[path] = &FileInfo{
			RefCount: 1,
			Handles:  make(map[int]types.ChunkHandle),
		}
	}

	ck := ChunkInfo{
		Version:  meta.Version,
		CheckSum: int64(meta.CheckSum),
		Path:     path,
	}
	file.Handles[len(file.Handles)] = meta.ChunkHandle
	cc.Chunk[meta.ChunkHandle] = &ck
	return nil
}

func (cc *ChunkControlor) MustChangeChunk(do *wal.LogOpLet, handle types.ChunkHandle, ckc types.PersiteChunkInfo) error {
	ck := cc.Chunk[handle]

	log := types.ChunkLogImpl{
		CommandType: types.CommandUpdate,
		Path:        ck.Path,
		Chunk:       ckc,
	}
	ctx, h := context.WithTimeout(context.TODO(), 1*time.Second)
	defer h()
	return do.ChunkStartCtx(ctx, log)
}

func (cc *ChunkControlor) applyChangeChunk(path types.Path, meta types.PersiteChunkInfo) error {
	cc.RLock()
	file, ok := cc.Files[path]
	if !ok {
		cc.RUnlock()
		return types.ErrNotExist
	}
	idx := file.HasChunk(meta.ChunkHandle)
	if idx == -1 {
		cc.RUnlock()
		return types.ErrNotExist
	}
	ck, ok := cc.Chunk[meta.ChunkHandle]
	if !ok {
		cc.RUnlock()
		return types.ErrNotExist
	}
	cc.RUnlock()
	file.Lock()
	defer file.Unlock()
	if file.Handles[idx] != meta.ChunkHandle {
		cc.Lock()
		defer cc.Unlock()
		handle := file.Handles[idx]
		ckc := cc.Chunk[handle]
		delete(cc.Chunk, file.Handles[idx])
		file.Handles[idx] = meta.ChunkHandle
		cc.Chunk[meta.ChunkHandle] = ckc
		ck = ckc
	}
	ck.Lock()
	defer ck.Unlock()

	ck.CheckSum = int64(meta.CheckSum)
	ck.Version = meta.Version
	return nil
}
func (cc *ChunkControlor) MustRemoveChunk(do *wal.LogOpLet, handle types.ChunkHandle) error {
	ck := cc.Chunk[handle]
	log := types.ChunkLogImpl{
		CommandType: types.CommandDelete,
		Path:        ck.Path,
		Chunk: types.PersiteChunkInfo{
			ChunkHandle: handle,
		},
	}
	ctx, _ := context.WithTimeout(context.TODO(), 1*time.Second)
	return do.ChunkStartCtx(ctx, log)
}
func (cc *ChunkControlor) applyRemoveChunk(path types.Path, meta types.PersiteChunkInfo) error {
	cc.Lock()
	defer cc.Unlock()
	// lock ?
	delete(cc.Files, path)
	delete(cc.Chunk, meta.ChunkHandle)
	return nil
}

// logic
func (cc *ChunkControlor) GetChunk(path types.Path, chunkIndex int) (types.ChunkHandle, error) {
	cc.RLock()
	f, ok := cc.Files[path]
	cc.RUnlock()
	if !ok {
		return -1, fmt.Errorf("not found file %v", path)
	}
	f.RLock()
	defer f.RUnlock()
	// 暂时步考虑写时复制功能

	if chunkIndex < 0 || chunkIndex >= len(f.Handles) {
		return -1, fmt.Errorf("invalid index %v", chunkIndex)
	}
	handle, ok := f.Handles[chunkIndex]
	if !ok {
		return -1, types.ErrChunkAllLose
	}
	return handle, nil
}
func (cc *ChunkControlor) RegisterReplicas(handle types.ChunkHandle, server types.Addr) error {
	cc.RLock()
	ck, ok := cc.Chunk[handle]
	cc.RUnlock()
	if !ok {
		return fmt.Errorf("not found chunk %v", handle)
	}
	ck.Lock()
	defer ck.Unlock()
	ck.Replicas = append(ck.Replicas, server)
	return nil
}

// 对path文件初始化一个chunk
// 调用前必须检查ns下对应的文件存在
// 返回handle,replica对应的server和错误
func (cc *ChunkControlor) CreateChunk(do *wal.LogOpLet, path types.Path, servers []types.Addr) (types.ChunkHandle, []types.Addr, []error) {
	cc.Lock()
	_, ok := cc.Files[path]
	if !ok {
		//第一次创建
		cc.Files[path] = &FileInfo{
			RefCount: 1,
			Handles:  make(map[int]types.ChunkHandle),
		}
	}
	cc.Unlock()
	handle, err := cc.MustCreateChunk(do, cc.Files[path], path)
	if err != nil || err != types.ErrFine {
		panic(err)
	}

	// f.Lock()
	// handle := types.ChunkHandle(common.GetChunkHandleId())
	// f.Handles[len(f.Handles)] = types.ChunkHandle(handle)
	// f.Unlock()
	// // create chunk
	// ck := &ChunkInfo{
	// 	Path:    path,
	// 	Version: 1,
	// 	Expire:  time.Now(), // 初始化租约信息
	// }

	// cc.OperationLog = append(cc.OperationLog, types.ChunkLogImpl{
	// 	CommandType: types.CommandCreate,
	// 	Path:        path,
	// 	Chunk: types.PersiteChunkInfo{
	// 		Version:     1,
	// 		ChunkHandle: handle,
	// 	},
	// })
	// // wal.Start()
	// cc.Chunk[handle] = ck
	// cc.Unlock()
	cc.Lock()
	ck := cc.Chunk[handle]
	cc.Unlock()
	errs := []error{}
	var wg sync.WaitGroup
	for _, v := range servers {
		wg.Add(1)
		go func(peer types.Addr) {
			var resp types.CreateChunkReply
			args := types.CreateChunkArg{
				Handle: handle,
			}
			err := rpc.Call(peer, "ChunkServer.CreateChunk", &args, &resp)
			ck.Lock()
			defer ck.Unlock()
			if err != nil {
				errs = append(errs, err)
			} else {
				ck.Replicas = append(ck.Replicas, peer)
			}
			wg.Done()
		}(v)
	}

	wg.Wait()
	cc.Lock()
	defer cc.Unlock()
	if len(errs) != 0 {
		cc.replicaNeedList = append(cc.replicaNeedList, handle)
	}

	return handle, cc.Chunk[handle].Replicas, errs
}

// 移除server的有关的handles信息
// 如果检测到接近阀值会调度分片
func (cc *ChunkControlor) RemoveChunk(do *wal.LogOpLet, handles []types.ChunkHandle, server types.Addr) error {
	cc.Lock()

	for _, handle := range handles {
		ck := cc.Chunk[handle]
		ck.Lock()
		defer ck.Unlock()
		for i, v := range ck.Replicas {
			if v == server {
				ck.Replicas = append(ck.Replicas[:i], ck.Replicas[i+1:]...)
			}
		}
		num := len(cc.Chunk[handle].Replicas)
		if num < common.MinChunks {
			cc.replicaNeedList = append(cc.replicaNeedList, handle)
			if num == 0 {
				// log
				err := cc.MustRemoveChunk(do, handle)
				if err != nil {
					panic(err)
				}
				//log.Println("lose all replica of %v", handle)
				common.LWarn("lose all replica of %v", handle)
				for _, v := range cc.Files {
					if index := v.HasChunk(handle); index != -1 {
						delete(v.Handles, index)
					}
				}
			}
		}
	}

	cc.Unlock()

	return nil
}

func (cc *ChunkControlor) GetReplicas(handle types.ChunkHandle) ([]types.Addr, error) {
	cc.RLock()
	ck, ok := cc.Chunk[handle]
	cc.RLock()
	if !ok {
		return nil, fmt.Errorf("not found chunk %v", handle)
	}

	return ck.Replicas, nil
}

// 获取Lease信息
func (cc *ChunkControlor) GetLease(do *wal.LogOpLet, handle types.ChunkHandle) (*Lease, []types.Addr, error) {
	cc.RLock()
	ck, ok := cc.Chunk[handle]
	cc.RUnlock()

	if !ok {
		return nil, nil, fmt.Errorf("invalid chunk %v", handle)
	}
	// 有可能触发COW
	ck.Lock()
	lease := Lease{}
	oldServers := []types.Addr{}
	// chunk 的租约过期
	// 根据cs持有的version
	if ck.Expire.Before(time.Now()) {
		ckc := types.PersiteChunkInfo{
			Version:     ck.Version + 1,
			CheckSum:    int(ck.CheckSum),
			ChunkHandle: handle,
		}

		err := cc.MustChangeChunk(do, handle, ckc)
		if err != nil {
			panic(err)
		}
		cc.RLock()
		ck := cc.Chunk[handle]
		cc.RUnlock()

		arg := types.CheckReplicaVersionArg{
			Handle:  handle,
			Version: ck.Version,
		}
		ck.Unlock()
		var wg sync.WaitGroup
		mu := sync.Mutex{}
		newList := []types.Addr{}
		for _, v := range ck.Replicas {
			wg.Add(1)
			go func(peer types.Addr) {
				var reply types.CheckReplicaVersionReply
				err := rpc.Call(peer, "ChunkServer.CheckReplicaVersion", &arg, &reply)
				mu.Lock()
				defer mu.Unlock()
				if err == nil && !reply.IsStale {
					newList = append(newList, peer)
				} else {
					log.Printf("stale replica %v from %v", handle, peer)
					oldServers = append(oldServers, peer)
				}
				wg.Done()
			}(v)
		}

		wg.Wait()
		ck.Lock()
		defer ck.Unlock()
		ck.Replicas = make([]types.Addr, 0)
		ck.Replicas = append(ck.Replicas, newList...)
		if len(ck.Replicas) < common.MinChunks {
			cc.Lock()
			cc.replicaNeedList = append(cc.replicaNeedList, handle)
			cc.Unlock()
			if len(ck.Replicas) == 0 {
				ck.Version-- // why?
				log.Printf("lose all replicas %v", handle)
			}
		}

		// schedule policy
		// 维护分片访问次数，在版本相同时，优先选择访问次数少的
		// 维护分片近期访问次数，在版本相同时，优先选择近期最少访问的
		rand.Seed(time.Now().Unix())
		n := rand.Intn(len(ck.Replicas))
		ck.Primary = ck.Replicas[n]
		ck.Expire = time.Now().Add(time.Duration(common.ExpireTimeout) * time.Second)
	}
	ck.Unlock()
	return &lease, oldServers, nil
}

// 整理ChunkControlor的NeedList
// 返回保证不会重复handle
// 返回缺少分片的Chunk
func (cc *ChunkControlor) GetNeedList() []types.ChunkHandle {
	cc.Lock()
	defer cc.Unlock()

	newList := []int{}
	for _, v := range cc.replicaNeedList {
		if len(cc.Chunk[v].Replicas) < common.MinChunks {
			newList = append(newList, int(v))
		}
	}
	// [0 1 1 2 4 6 6 9 13] -> [0 1 2 4 6 9 13]
	sort.Ints(newList)

	cc.replicaNeedList = make([]types.ChunkHandle, 0)
	for i, v := range newList {
		if i == 0 || v != newList[i-1] {
			cc.replicaNeedList = append(cc.replicaNeedList, types.ChunkHandle(v))
		}
	}

	return cc.replicaNeedList
}
