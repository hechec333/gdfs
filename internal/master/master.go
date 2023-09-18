package master

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"gdfs/internal/common"
	xrpc "gdfs/internal/common/rpc"
	"gdfs/internal/types"
	"gdfs/internal/wal"
	"gdfs/internal/wal/raft"
	"log"
	"net"
	"net/rpc"
	"os"
	"runtime/debug"
	"sync"
	"time"
)

type Master struct {
	sync.RWMutex
	me          int // wal.Role()
	masterAddrs []types.Addr
	dead        bool
	shutdown    chan struct{}
	//logger *LogOperationControlor
	cc  *ChunkControlor
	csc *ChunkServerControlor
	ns  *NameSpaceControlor
	wal *wal.WriteAheadLog
}

// implement ICommandLet
// first error return sys error
// second  error return apply command error
func (m *Master) ServeApplyCommand(msg raft.ApplyMsg) (error, error) {
	m.Lock()
	defer m.Unlock()

	op := msg.Command.(wal.LogOp)

	err := m.dispatch(&op)
	return nil, err
}
func (m *Master) chunkDispath(log *types.ChunkLogImpl) error {
	switch log.CommandType {
	case types.CommandCreate:
		return m.cc.applyCreateChunk(log.Path, log.Chunk)
	case types.CommandDelete:
		return m.cc.applyRemoveChunk(log.Path, log.Chunk)
	case types.CommandUpdate:
		return m.cc.applyChangeChunk(log.Path, log.Chunk)
	default:
		return types.ErrUnknownOperation
	}
}
func (m *Master) nsDispatch(log *types.NsLogImpl) error {
	switch log.CommandType {
	case types.CommandCreate:
		return m.ns.applyCreate(log.Path, log.File)
	case types.CommandDelete:
		return m.ns.applyDelete(log.Path, log.File)
	case types.CommandUpdate:
		return m.ns.applyChangeFileInfo(log.Path, log.File)
	default:
		return types.ErrUnknownOperation
	}
}
func (m *Master) dispatch(log *wal.LogOp) error {
	if log.OpType == types.ChunkLog {
		return m.chunkDispath(log.Log.(*types.ChunkLogImpl))
	} else if log.OpType == types.NsLog {
		return m.nsDispatch(log.Log.(*types.NsLogImpl))
	} else {
		return types.ErrUnknownLog
	}
}
func (m *Master) ServeSnapShot() ([]byte, error) {
	nsbt := m.ns.SavePersiteState()
	ccbt := m.cc.SavePersiteState()

	buf := new(bytes.Buffer)
	e := gob.NewEncoder(buf)
	if e.Encode(&nsbt) != nil || e.Encode(&ccbt) != nil {
		return nil, fmt.Errorf("gob encode master state error")
	}
	return buf.Bytes(), nil
}
func (m *Master) InstallSnapShot(snap []byte) error {
	nsbt := []types.PersiteTreeNode{}
	ccbt := types.PersiteChunkControlor{}
	buf := bytes.NewBuffer(snap)
	dec := gob.NewDecoder(buf)
	if dec.Decode(&nsbt) != nil || dec.Decode(&ccbt) != nil {
		return fmt.Errorf("gob decode master state error")
	}
	err := m.ns.ReadPersiteState(nsbt)
	if err != nil {
		return err
	}
	err = m.cc.ReadPersiteState(ccbt)
	return err
}

//

// Master日志只需要记录到master自身数据的改变行为
// 1.涉及Chunk的增删查改 关键信息如Version,CheckSum,Length。 日志 Chunk.Create Chunk.Update Chunk.Delete
// 2.涉及NameSpace的增删

// 所有涉及Master的Voliate-state都必须经过WAL模块
// 所有涉及Master的Non-Voliate-state都必须在竞选为Leader后重置，并且所有的状态都是由chunkserver提供包括不限于1.chunk的replica信息，2.chunkserver的位置信息
const (
	MetaFileName = "types-master.meta"
	FilePerm     = 0755
)

func MustNewAndServe(config *types.MetaServerServeConfig) *Master {
	m := &Master{
		me:          config.Me,
		masterAddrs: config.Servers,
		shutdown:    make(chan struct{}),
		cc:          NewChunkControlor(),
		csc:         NewChunkServerControlor(),
		ns:          NewNamespaceControlor(),
		dead:        false,
	}

	m.init()

	server := rpc.NewServer()
	server.Register(m)

	l, err := net.Listen("tcp", string(m.masterAddrs[m.me]))
	if err != nil {
		panic(err)
	}
	go xrpc.NewRpcAndServe(server, l, m.shutdown, xrpc.AcceptWithTimeOut(10*time.Second))
	go m.GoBackgroundTask()
	return m
}
func (m *Master) Stop() {
	m.shutdown <- struct{}{}
}
func (m *Master) init() {
	cwd, _ := os.Getwd()
	ips := wal.NewDefaultPersiter(cwd)
	if ips == nil {
		panic("new persiter error")
	}
	m.cc.ClearVolitateStateAndRebuild()
	m.csc.ClearAllVoliateStateAndRebuild()
	peers := xrpc.NewClientPeers(m.masterAddrs)
	m.wal = wal.StartWalPeer(m, peers, m.me, ips, common.MaxRaftState)
}

func (m *Master) GoBackgroundTask() {
	snapTicker := time.NewTicker(common.SnapInterval)
	scanTicker := time.NewTicker(common.LazyCollectInterval)
	checkTicker := time.NewTicker(common.CheckInterval)
	for {
		var err error
		select {
		case <-snapTicker.C:
			m.wal.NotifySnapShot()
		case <-scanTicker.C:
			err = m.lazyCollector()
		case <-checkTicker.C:
			err = m.serverCheck()
		case <-m.shutdown:
			return
		}
		if err != nil {
			log.Printf("[WARN] BACKGROUND Error %v", err)
		}
	}
}

func (m *Master) lazyCollector() error {
	defer func() {
		err := recover()
		if err != nil {
			log.Printf("[WARN] error:%v,debug:%v", err, string(debug.Stack()))
		}
	}()
	let := wal.NewLogOpLet(m.wal, int64(m.me), 0, "internal")
	files, err := m.ns.FetchAllDeletedFiles(let)
	if err != nil {
		return err
	}
	if len(files) == 0 {
		return nil
	}
	m.cc.RLock()
	defer m.cc.RUnlock()
	srvs := make(map[types.Addr][]types.ChunkHandle)
	for _, v := range files {
		origin := common.Markdemangle(string(v.Path))
		info := m.cc.Files[types.Path(origin)]
		info.RLock()
		defer info.RUnlock()
		for _, v := range info.Handles {
			for _, vv := range m.cc.Chunk[v].Replicas {
				if _, ok := srvs[vv]; !ok {
					srvs[vv] = make([]types.ChunkHandle, v)
					continue
				}
				srvs[vv] = append(srvs[vv], v)
			}
		}
	}

	for addr, chunks := range srvs {
		for _, v := range chunks {
			m.csc.AddGarbage(addr, v)
		}
	}
	return nil
}
func (m *Master) serverCheck() error {
	// lazy delete
	defer func() {
		err := recover()
		if err != nil {
			log.Printf("[WARN] error:%v,debug:%v", err, string(debug.Stack()))
		}
	}()
	let := wal.NewLogOpLet(m.wal, int64(m.me), 0, "internal")
	// detect dead servers
	addrs := m.csc.GetDeadServers()

	// sync

	for _, v := range addrs {
		//log.Warningf("remove server %v", v)
		handles := m.csc.RemoveServer(v)
		if handles != nil {
			err := m.cc.RemoveChunk(let, handles, v)
			if err != nil {
				return err
			}
		}
	}

	// add replicas for need request
	handles := m.cc.GetNeedList()
	if handles != nil {
		//log.Info("Master Need ", handles)
		m.cc.RLock()
		for i := 0; i < len(handles); i++ {
			ck := m.cc.Chunk[handles[i]]

			if ck.Expire.Before(time.Now()) { //一定要过期才能执行复制？
				ck.Lock() // don't grant lease during copy
				m.reReplication(handles[i])
				//log.Info(err)
				ck.Unlock()
			}
		}
		m.cc.RUnlock()
	}

	return nil
}

func (m *Master) RPCMasterCheck(arg *types.MasterCheckArg, reply *types.MasterCheckReply) error {
	reply.Server = m.masterAddrs[m.me]
	reply.Master = false
	if term, is := m.wal.RoleState(); is {
		reply.Master = true
		reply.Term = term
	}
	return nil
}

// reReplication performs re-replication, ck should be locked in top caller
// new lease will not be granted during copy
func (m *Master) reReplication(handle types.ChunkHandle) error {
	// chunk are locked, so master will not grant lease during copy time
	from, to, err := m.csc.CopyChunk(handle)
	if err != nil {
		return err
	}
	//log.Warningf("allocate new chunk %v from %v to %v", handle, from, to)

	var cr types.CreateChunkReply
	ccArg := types.CreateChunkArg{
		Handle: handle,
	}
	err = xrpc.Call(to, "ChunkServer.RPCCreateChunk", &ccArg, &cr)
	if err != nil {
		return err
	}

	var sr types.SendCopyReply
	scArg := types.SendCopyArg{
		Handle:  handle,
		Address: to,
	}
	err = xrpc.Call(from, "ChunkServer.RPCSendCopy", &scArg, &sr)
	if err != nil {
		return err
	}

	m.cc.RegisterReplicas(handle, to)
	m.csc.AddChunk([]types.Addr{to}, handle)
	return nil
}

// RPCHeartbeat is called by chunkserver to let the master know that a chunkserver is alive
func (m *Master) RPCHeartbeat(args types.HeartbeatArg, reply *types.HeartbeatReply) error {
	_, is := m.wal.RoleState()
	if !is {
		reply.Redirect = true
		return nil
	}
	isFirst := m.csc.HeartBeat(args.Address, reply)

	if isFirst { // if is first heartbeat, let chunkserver report itself
		var r types.ReportSelfReply
		rsArg := types.ReportSelfArg{}
		err := xrpc.Call(args.Address, "ChunkServer.RPCReportSelf", &rsArg, &r)
		if err != nil {
			return err
		}

		for _, v := range r.Chunks {
			m.cc.RLock()
			ck, ok := m.cc.Chunk[v.ChunkHandle]
			if !ok {
				// 没找到
				continue
			}
			version := ck.Version
			m.cc.RUnlock()

			if v.Version == version {
				//log.Infof("Master receive chunk %v from %v", v.Handle, args.Address)
				m.cc.RegisterReplicas(v.ChunkHandle, args.Address)
				m.csc.AddChunk([]types.Addr{args.Address}, v.ChunkHandle)
			} else {
				//log.Infof("Master discard %v", v.Handle)
				log.Printf("Master discard %v", v.ChunkHandle)
			}
		}
	}
	return nil
}

// RPCGetPrimaryAndSecondaries returns lease holder and secondaries of a chunk.
// If no one holds the lease currently, grant one.
// Master will communicate with all replicas holder to check version, if stale replica is detected, add it to garbage collection
func (m *Master) RPCGetPrimaryAndSecondaries(args types.GetPrimaryAndSecondariesArg, reply *types.GetPrimaryAndSecondariesReply) error {
	let := wal.NewLogOpLet(m.wal, args.ClientId, args.Seq, "client")
	defer func() {
		err := recover()
		reply.Err = err.(error)
	}()

	lease, staleServers, err := m.cc.GetLease(let, args.Handle)
	if err != nil {
		return err
	}

	for _, v := range staleServers {
		m.csc.AddGarbage(v, args.Handle)
	}

	reply.Primary = lease.Primary
	reply.Expire = lease.Expire

	for _, v := range lease.Location {
		if v != lease.Primary {
			reply.Secondaries = append(reply.Secondaries, v)
		}
	}
	return nil
}

// // RPCExtendLease extends the lease of chunk if the lessee is nobody or requester.
// func (m *Master) RPCExtendLease(args types.ExtendLeaseArg, reply *types.ExtendLeaseReply) error {
// 	//t, err := m.cm.ExtendLease(args.Handle, args.Address)
// 	//if err != nil { return err }
// 	//reply.Expire = *t
// 	return nil
// }

// RPCGetReplicas is called by client to find all chunkserver that holds the chunk.
func (m *Master) RPCGetReplicas(args types.GetReplicasArg, reply *types.GetReplicasReply) error {
	servers, err := m.cc.GetReplicas(args.Handle)
	if err != nil {
		return err
	}
	reply.Locations = append(reply.Locations, servers...)
	// for _, v := range servers {
	// 	reply.Locations = append(reply.Locations, v)
	// }
	return nil
}

// RPCCreateFile is called by client to create a new file
func (m *Master) RPCCreateFile(args types.CreateFileArg, reply *types.CreateFileReply) (err error) {
	defer func() {
		errz := recover()
		err = errz.(error)
	}()
	let := wal.NewLogOpLet(m.wal, args.ClientId, args.Seq, "client")
	reply.Err = m.ns.CreateFileImpl(let, args.Path)
	return
}

// RPCDelete is called by client to delete a file
func (m *Master) RPCDeleteFile(args types.DeleteFileArg, reply *types.DeleteFileReply) (err error) {
	defer func() {
		errz := recover()
		err = errz.(error)
	}()
	let := wal.NewLogOpLet(m.wal, args.ClientId, args.Seq, "client")
	reply.Err = m.ns.DeleteFileImpl(let, args.Path)
	return
}

// RPCRename is called by client to rename a file
func (m *Master) RPCRenameFile(args types.RenameFileArg, reply *types.RenameFileReply) error {

	return nil
}

// RPCMkdir is called by client to make a new directory
func (m *Master) RPCMkdir(args types.MkdirArg, reply *types.MkdirReply) (err error) {
	defer func() {
		errz := recover()
		err = errz.(error)
	}()
	let := wal.NewLogOpLet(m.wal, args.ClientId, args.Seq, "client")
	reply.Err = m.ns.MkdirImpl(let, args.Path)
	return
}

// RPCList is called by client to list all files in specific directory
func (m *Master) RPCList(args types.ListArg, reply *types.ListReply) error {
	var err error
	reply.Files, err = m.ns.GetList(args.Path)
	return err
}

// RPCGetFileInfo is called by client to get file information
func (m *Master) RPCGetFileInfo(args types.GetFileInfoArg, reply *types.GetFileInfoReply) error {
	info, err := m.ns.GetFileInfoImpl(args.Path)
	if info.IsDir {
		err = fmt.Errorf("path %v is dir", args.Path)
	}
	reply.Chunks = info.Chunks
	reply.Length = info.Length
	return err
}

// RPCGetChunkHandle returns the chunk handle of (path, index).
// If the requested index is bigger than the number of chunks of this path by one, create one.
func (m *Master) RPCGetChunkHandle(args types.GetChunkHandleArg, reply *types.GetChunkHandleReply) (err error) {
	_, cwd, err := m.ns.lockUpperPath(args.Path, true)
	defer m.ns.unlockUpperPath(args.Path)
	if err != nil {
		reply.Err = err
		return
	}
	cwd.RLock()
	defer cwd.RUnlock()
	file, ok := cwd.GetChild(common.GetFileNameWithExt(args.Path))
	// append new chunks
	if !ok {
		reply.Err = os.ErrNotExist
	}
	file.Lock()
	defer file.Unlock()
	let := wal.NewLogOpLet(m.wal, args.ClientId, args.Seq, "client")
	if int(args.Index) == int(file.chunks) { //如果不够就创建,惰性策略的一种体现
		defer func() {
			errz := recover()
			err = errz.(error)
		}()
		//file.chunks++
		errz := file.MustAddChunks(let, args.Path, 1)
		if errz != nil {
			panic(errz)
		}
		//
		addrs, errz := m.csc.ScheduleActiveServers(common.ReplicasLevel)
		if err != nil && len(addrs) == 0 {
			reply.Err = errz
			return
		}
		var errs []error
		reply.Handle, addrs, errs = m.cc.CreateChunk(let, args.Path, addrs)
		if len(errs) == len(addrs) {
			// WARNING
			//log.Warning("[ignored] An ignored error in RPCGetChunkHandle when create ", err, " in create chunk ", reply.Handle)
			log.Printf("Create Chunk error in %v", addrs)
			reply.Err = errors.Join(errs...)
			return
		}

		m.csc.AddChunk(addrs, reply.Handle)
	} else {
		reply.Handle, err = m.cc.GetChunk(args.Path, args.Index)
	}

	return
}
