package master

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"gdfs/internal/common"
	xrpc "gdfs/internal/common/rpc"
	"gdfs/internal/wal"
	"gdfs/internal/wal/raft"
	"gdfs/types"
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
	who         int // master index
	masterAddrs []types.Addr
	protocols   []types.Addr
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
	common.LTrace("<Master%v> receive apply command from raft", m.me)
	op := msg.Command.(wal.LogOp)

	err := m.dispatch(&op)
	return nil, err
}
func (m *Master) chunkDispath(log types.ChunkLogImpl) error {
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
func (m *Master) opDispatch(log types.BatchLogImpl) error {
	switch log.Type {
	case types.NsLog:
		ins := make([]types.NsLogImpl, len(log.Batch))

		for idx, v := range log.Batch {
			ins[idx] = v.(types.NsLogImpl)
		}
		return m.batchDispatch(ins)
	case types.ChunkLog:
		fallthrough
	default:
		return types.ErrUnknownOperation
	}
}

func (m *Master) batchDispatch(ins []types.NsLogImpl) error {
	tpo := ins[0].CommandType

	switch tpo & 0xffff {
	case types.CommandCreate:
		pp := ins[0].Path
		if tpo&types.OP_FILE == types.OP_FILE {
			coms := make([]types.PersiteTreeNode, len(ins))
			for idx, v := range ins {
				coms[idx] = v.File
			}
			return m.ns.applyCreatef(pp, coms)
		}

		if tpo&types.OP_DIC == types.OP_DIC {
			coms := make([][]types.PersiteTreeNode, len(ins))

			for idx, v := range ins {
				coms[idx] = v.Dics
			}

			return m.ns.applyBatchMkdir(pp, coms)
		}

		return types.ErrUnReachAble
	case types.CommandDelete:
		pp := ins[0].Path
		coms := make([]types.PersiteTreeNode, len(ins))
		for idx, v := range ins {
			coms[idx] = v.File
		}
		return m.ns.applyDelete(pp, coms)
	}

	return types.ErrInvalidArgument
}

func (m *Master) nsDispatch(log types.NsLogImpl) error {
	switch log.CommandType & 0xffff {
	case types.CommandCreate:
		if log.CommandType&types.OP_FILE == types.OP_FILE {
			return m.ns.applyCreatef(log.Path, []types.PersiteTreeNode{log.File})
		}
		if log.CommandType&types.OP_DIC == types.OP_DIC {
			return m.ns.applyCreated(log.Path, log.Dics)
		}
	case types.CommandDelete:
		return m.ns.applyDelete(log.Path, []types.PersiteTreeNode{log.File})
	case types.CommandUpdate:
		return m.ns.applyChangeFileInfo(log.Path, log.File)
	default:
		return types.ErrUnknownOperation
	}

	return types.ErrUnknownOperation
}
func (m *Master) dispatch(log *wal.LogOp) error {
	if log.OpType == types.ChunkLog {
		return m.chunkDispath(log.Log.(types.ChunkLogImpl))
	} else if log.OpType == types.NsLog {
		return m.nsDispatch(log.Log.(types.NsLogImpl))
	} else if log.OpType == types.BatchLog {
		return m.opDispatch(log.Log.(types.BatchLogImpl))
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
// 所有涉及Master的Non-Voliate-state都必须在竞选为Leader后重置，并且所有的状态都是由chunkserver提供包括不限于
// 1.chunk的replica信息，2.chunkserver的位置信息
const (
	MetaFileName = "types-master.meta"
	FilePerm     = 0755
)

func MustNewAndServe(config *types.MetaServerServeConfig) *Master {
	m := &Master{
		me:          config.Me,
		masterAddrs: config.Servers,
		protocols:   config.Protocol,
		shutdown:    make(chan struct{}),
		cc:          NewChunkControlor(),
		csc:         NewChunkServerControlor(),
		ns:          NewNamespaceControlor(),
		dead:        false,
	}

	m.init()

	mserver := rpc.NewServer()
	mserver.Register(m)

	l, err := net.Listen("tcp", string(m.masterAddrs[m.me]))
	if err != nil {
		panic(err)
	}
	common.LInfo("starting rpc service port on %v", m.masterAddrs[m.me])
	go m.wal.SubscribeRoleChange(func(l bool, who int) {
		common.LWarn("<Master%v> role change new leader %v", who)
		m.who = who
	})
	go xrpc.NewRpcAndServe(mserver, l, m.shutdown, xrpc.AcceptWithTimeOut(10*time.Second))
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
	if len(m.masterAddrs) == 1 {
		common.LInfo("init master clusters in standlone")
	}
	if len(m.masterAddrs)%2 == 0 {
		common.LWarn("odd master cluster detecded!")
	}

	peers := xrpc.NewClientPeers(m.protocols)
	m.wal = wal.StartWalPeer(m, peers, m.me, ips, common.MaxRaftState)

	server := rpc.NewServer()
	server.Register(m.wal.Getrf())
	l, err := net.Listen("tcp", string(m.protocols[m.me]))
	if err != nil {
		panic(err)
	}
	common.LInfo("starting quromn address in %v", m.protocols[m.me])
	go xrpc.NewRpcAndServe(server, l, m.shutdown)
}

func (m *Master) GoBackgroundTask() {
	snapTicker := time.NewTicker(common.SnapInterval)
	scanTicker := time.NewTicker(common.LazyCollectInterval)
	checkTicker := time.NewTicker(common.CheckInterval)
	common.LInfo("<Master%v> init background taskgroup [snapshot,scanning,selfcheck]", m.me)
	for {
		var err error
		select {
		case <-snapTicker.C:
			common.LInfo("<Master%v> ready to save snapshot", m.me)
			m.wal.NotifySnapShot()
		case <-scanTicker.C:
			common.LInfo("<Master%v> ready to begin garbage collect", m.me)
			err = m.lazyCollector()
		case <-checkTicker.C:
			common.LInfo("<Master%v> ready to begin self check", m.me)
			err = m.serverCheck()
		case <-m.shutdown:
			return
		}
		if err != nil {
			//log.Printf("[WARN] BACKGROUND Error %v", err)
			common.LWarn("background task error %v", err)
		}
	}
}

func (m *Master) lazyCollector() error {
	defer func() {
		if err := recover(); err != nil {
			common.LWarn("ignored! error %v debug %v", err, string(debug.Stack()))
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
		if err := recover(); err != nil {
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

func (m *Master) CheckLeader() bool {
	_, l := m.wal.RoleState()
	if m.who != m.me || !l {
		return false
	}
	return true
}

func (m *Master) Redirect() bool {
	rf := m.wal.Getrf()
	return rf.Lease.OnLease()
}

func (m *Master) WaitForLoaclRead() error {
	spins := 0
	for !m.wal.CheckLocalReadStat() {
		common.LWarn("<Master%v> not in leader lease time", m.me)
		time.Sleep(10 * time.Millisecond)
		spins++
		if spins >= 5 {
			return types.ErrRedirect
		}
	}
	return nil
}

func (m *Master) RPCCheckMaster(arg *types.MasterCheckArg, reply *types.MasterCheckReply) error {
	//common.LInfo("receive master check from %v", arg.Server)
	reply.Server = m.masterAddrs[m.me]
	reply.Master = false
	if term, is := m.wal.RoleState(); is {
		reply.Master = true
		reply.Term = term
	}
	common.LInfo("receive master check from %v,master %v", arg.Server, reply.Master)
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
	common.LInfo("<Master%v> heartbeat from chunkserver %v", m.me, args.Address)
	is := m.CheckLeader()
	if !is {
		reply.Redirect = true
		return nil
	}
	isFirst := m.csc.HeartBeat(args.Address, reply)

	if isFirst { // if is first heartbeat, let chunkserver report itself
		common.LInfo("new join chunkserver %v", args.Address)
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
				common.LTrace("<Master%v> commit chunk %v from ck %v", m.me, v.ChunkHandle, args.Address)
				m.cc.RegisterReplicas(v.ChunkHandle, args.Address)
				m.csc.AddChunk([]types.Addr{args.Address}, v.ChunkHandle)
			} else {
				common.LTrace("<Master%v> discard chunk %v,inequal version set", m.me, v.ChunkHandle)
			}
		}
		// 更新master的property
		m.csc.UpdateServerProperty(args.Address, r.Pro)
	}
	return nil
}

// RPCGetPrimaryAndSecondaries returns lease holder and secondaries of a chunk.
// If no one holds the lease currently, grant one.
// Master will communicate with all replicas holder to check version, if stale replica is detected, add it to garbage collection
func (m *Master) RPCGetPrimaryAndSecondaries(args types.GetPrimaryAndSecondariesArg, reply *types.GetPrimaryAndSecondariesReply) error {
	let := wal.NewLogOpLet(m.wal, args.ClientIdentity.ClientId, args.ClientIdentity.Seq, "client")
	defer func() {
		if err := recover(); err != nil {
			reply.Err = types.NewError(err.(error))
		}
	}()

	lease, staleServers, err := m.cc.GetLease(let, args.Handle)
	if err != nil {
		return err
	}

	for _, v := range staleServers {
		m.csc.AddGarbage(v, args.Handle)
	}

	reply.Primary = types.EndpointInfo{
		Addr:     lease.Primary,
		Property: m.csc.GetServerProperty(lease.Primary),
	}
	reply.Expire = lease.Expire

	for _, v := range lease.Location {
		if v != lease.Primary {
			reply.Secondaries = append(reply.Secondaries, types.EndpointInfo{
				Addr:     v,
				Property: m.csc.GetServerProperty(v),
			})
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
	if !m.CheckLeader() {
		return types.ErrRedirect
	}
	err := m.WaitForLoaclRead()
	if err != nil {
		return err
	}
	servers, err := m.cc.GetReplicas(args.Handle)
	if err != nil {
		return err
	}
	for _, server := range servers {
		reply.Locations = append(reply.Locations, types.EndpointInfo{
			Addr:     server,
			Property: m.csc.GetServerProperty(server),
		})
	}
	// for _, v := range servers {
	// 	reply.Locations = append(reply.Locations, v)
	// }
	return nil
}

// RPCCreateFile is called by client to create a new file
func (m *Master) RPCCreateFile(args types.CreateFileArg, reply *types.CreateFileReply) (err error) {
	defer func() {
		if errz := recover(); errz != nil {
			err = errz.(error)
		}
	}()
	if !m.CheckLeader() {
		return types.ErrRedirect
	}
	let := wal.NewLogOpLet(m.wal, args.ClientIdentity.ClientId, args.ClientIdentity.Seq, "client")
	return m.ns.CreateFileImpl(let, args.Path, args.Perm)
}

// RPCDelete is called by client to delete a file
func (m *Master) RPCDeleteFile(args types.DeleteFileArg, reply *types.DeleteFileReply) (err error) {
	defer func() {
		if errz := recover(); errz != nil {
			err = errz.(error)
		}
	}()
	if !m.CheckLeader() {
		return types.ErrRedirect
	}
	let := wal.NewLogOpLet(m.wal, args.ClientIdentity.ClientId, args.ClientIdentity.Seq, "client")
	reply.Err = types.NewError(m.ns.DeleteFileImpl(let, args.Path))
	return
}

// RPCRename is called by client to rename a file
func (m *Master) RPCRenameFile(args types.RenameFileArg, reply *types.RenameFileReply) error {

	return nil
}

// RPCMkdir is called by client to make a new directory
func (m *Master) RPCMkdir(args types.MkdirArg, reply *types.MkdirReply) (err error) {
	defer func() {
		if errz := recover(); errz != nil {
			err = errz.(error)
		}
	}()
	if !m.CheckLeader() {
		return types.ErrRedirect
	}

	let := wal.NewLogOpLet(m.wal, args.ClientIdentity.ClientId, args.ClientIdentity.Seq, "client")
	err = m.ns.MkdirImpl(let, args.Path, args.Cfg.Recursive)
	return
}

// RPCList is called by client to list all files in specific directory
func (m *Master) RPCList(args types.ListArg, reply *types.ListReply) error {
	if !m.CheckLeader() {
		return types.ErrRedirect
	}
	err := m.WaitForLoaclRead()
	if err != nil {
		return err
	}
	reply.Files, err = m.ns.GetList(args.Path)
	return err
}

// RPCGetFileInfo is called by client to get file information
func (m *Master) RPCGetFileInfo(args types.GetFileInfoArg, reply *types.GetFileInfoReply) error {
	if !m.CheckLeader() {
		return types.ErrRedirect
	}
	err := m.WaitForLoaclRead()
	if err != nil {
		return err
	}
	info, err := m.ns.GetFileInfoImpl(args.Path)
	if info.IsDir {
		err = fmt.Errorf("path %v is dir", args.Path)
	}
	reply.Chunks = info.Chunks
	reply.Length = info.Length
	return err
}

func (m *Master) RPCSetFilePerm(arg types.SetFilePermArg, reply *types.SetFilePermReply) (err error) {
	defer func() {
		if errz := recover(); errz != nil {
			err = errz.(error)
		}
	}()
	if !m.CheckLeader() {
		return types.ErrRedirect
	}

	let := wal.NewLogOpLet(m.wal, arg.ClientIdentity.ClientId, arg.ClientIdentity.Seq, "client")

	return m.ns.SetFilePerm(let, arg.Path, arg.Perm)
}

func (m *Master) RPCGetFilePerm(arg types.GetFilePermArg, reply *types.GetFilePermReply) error {
	if !m.CheckLeader() {
		return types.ErrRedirect
	}
	err := m.WaitForLoaclRead()
	if err != nil {
		return err
	}

	info, err := m.ns.GetFilePerm(arg.Path)

	if err != nil {
		return err
	}

	reply.Info = info

	return nil
}

func (m *Master) RPCGetFileDetail(arg *types.GetFileDetailArg, argv *types.GetFileDetailReply) error {
	if !m.CheckLeader() {
		return types.ErrRedirect
	}
	err := m.WaitForLoaclRead()
	if err != nil {
		return err
	}

	info, err := m.ns.GetFileInfoImpl(arg.Path)

	if err != nil {
		return err
	}

	argv.Length = info.Length

	max := info.Chunks

	// m.cc.RLock()
	// defer m.cc.RUnlock()
	handles := make([]types.ChunkHandle, max)

	for i := 0; i < int(max); i++ {
		handles[i], err = m.cc.GetChunk(arg.Path, i)
		if err != nil {
			return err
		}

		ck := m.cc.Chunk[handles[i]]
		backups := make([]types.Addr, 0)
		for x := 0; x < len(ck.Replicas); x++ {
			if ck.Replicas[i] != ck.Primary {
				backups = append(backups, ck.Replicas[i])
			}
		}
		argv.Chunks = append(argv.Chunks, types.ChunkDetail{
			Handle: handles[i],
			Lease: types.LeaseInfo{
				Primary: ck.Primary,
				Backups: backups,
				Exipre:  ck.Expire,
			},
			Version: int64(ck.Version),
		})
	}

	return nil
}

// RPCGetChunkHandle returns the chunk handle of (path, index).
// If the requested index is bigger than the number of chunks of this path by one, create one.
func (m *Master) RPCGetChunkHandle(args types.GetChunkHandleArg, reply *types.GetChunkHandleReply) (err error) {
	_, cwd, err := m.ns.lockUpperPath(args.Path, true)
	defer m.ns.unlockUpperPath(args.Path)
	if err != nil {
		reply.Err = types.NewError(err)
		return
	}
	cwd.RLock()
	defer cwd.RUnlock()
	file, ok := cwd.GetChild(common.GetFileNameWithExt(args.Path))
	// append new chunks
	if !ok {
		reply.Err = types.NewError(os.ErrNotExist)
	}
	file.Lock()
	defer file.Unlock()
	let := wal.NewLogOpLet(m.wal, args.ClientIdentity.ClientId, args.ClientIdentity.Seq, "client")
	if int(args.Index) == int(file.chunks) { //如果不够就创建,惰性策略的一种体现
		defer func() {
			errz := recover()
			if errz != nil {
				err = errz.(error)
			}
		}()
		//file.chunks++
		errz := file.MustAddChunks(let, args.Path, 1)
		if errz != nil {
			panic(errz)
		}
		//
		addrs, errz := m.csc.ScheduleActiveServers(common.ReplicasLevel)
		if err != nil && len(addrs) == 0 {
			reply.Err = types.NewError(errz)
			return
		}
		num := len(addrs)
		var errs []error
		reply.Handle, addrs, errs = m.cc.CreateChunk(let, args.Path, addrs)
		if len(errs) == num {
			// WARNING
			//log.Warning("[ignored] An ignored error in RPCGetChunkHandle when create ", err, " in create chunk ", reply.Handle)
			reply.Err = types.NewError(common.JoinErrors(errs...))
			common.LWarn("<Master%v> Create Chunk to %v errors %v", m.me, addrs, reply.Err)
			return
		}

		m.csc.AddChunk(addrs, reply.Handle)
	} else {
		reply.Handle, err = m.cc.GetChunk(args.Path, args.Index)
	}

	return
}

func (m *Master) RPCPathTest(arg types.PathExistArg, reply *types.PathExistReply) error {
	if !m.CheckLeader() {
		return types.ErrRedirect
	}
	err := m.WaitForLoaclRead()
	if err != nil {
		return err
	}

	reply.Ok, err = m.ns.PathTest(arg.Path)

	return err
}

func (m *Master) RPCPathExist(arg types.PathExistArg, reply *types.PathExistReply) (err error) {
	if !m.CheckLeader() {
		return types.ErrRedirect
	}
	err = m.WaitForLoaclRead()
	if err != nil {
		return err
	}

	reply.Ok, err = m.ns.PathExist(arg.Path, arg.Dir)

	return nil
}

func supportMethod(service string) error {
	switch service {
	case "touch":
		fallthrough
	case "mkdir":
		fallthrough
	case "rm":
		return nil
	default:
		return types.ErrInvalidArgument
	}
}

var BatchOpMap map[string]func(*BatchContext) error = make(map[string]func(*BatchContext) error)

func (m *Master) BatchOpInit() {
	BatchOpMap["touch"] = func(bc *BatchContext) error {
		args := make([]types.CreateFileArg, len(bc.Arg.Cmd))
		paths := make([]types.Path, len(bc.Arg.Cmd))
		for idx, v := range bc.Arg.Cmd {
			args[idx] = v.(types.CreateFileArg)
			paths[idx] = args[idx].Path
		}
		return m.ns.CreateBatchFileImpl(bc.Do, args[0].Perm, paths...)
	}

	BatchOpMap["mkdir"] = func(bc *BatchContext) error {
		paths := make([]types.Path, len(bc.Arg.Cmd))
		for idx, v := range bc.Arg.Cmd {
			paths[idx] = v.(types.CreateFileArg).Path
		}
		return m.ns.CreateBatchDicImpl(bc.Do, paths...)
	}

	BatchOpMap["rm"] = func(bc *BatchContext) error {
		paths := make([]types.Path, len(bc.Arg.Cmd))
		for idx, v := range bc.Arg.Cmd {
			paths[idx] = v.(types.CreateFileArg).Path
		}
		return m.ns.DeleteBatchFileImpl(bc.Do, paths...)
	}
}

type BatchContext struct {
	Do    *wal.LogOpLet
	Arg   types.BatchOpArg
	Reply *types.BatchOpReply
}

// 暂未支持
func (m *Master) RPCBatchOp(args *types.BatchOpArg, reply *types.BatchOpReply) (err error) {

	defer func() {
		if errz := recover(); errz != nil {
			err = errz.(error)
		}
	}()
	if !m.CheckLeader() {
		return types.ErrRedirect
	}

	let := wal.NewLogOpLet(m.wal, args.ClientIdentity.ClientId, args.ClientIdentity.Seq, "client")

	bc := BatchContext{
		Do:    let,
		Arg:   *args,
		Reply: reply,
	}

	if f, ok := BatchOpMap[args.Method]; ok {
		xerr := f(&bc)
		reply.Err = types.NewError(xerr)
	} else {
		err = types.ErrInvalidArgument
	}

	return
}

func (m *Master) RPCSnapView(arg types.SnapViewArg, argv *types.SnapViewReply) error {
	if !m.CheckLeader() {
		return types.ErrRedirect
	}
	err := m.WaitForLoaclRead()
	if err != nil {
		return err
	}
	// 获取指定结点的视图
	tree, err := m.ns.NodeScan(arg.Path, nil)

	if err != nil {
		return err
	}

	nodes := make([]types.NodeView, len(tree))
	for idx, v := range tree {
		nodes[idx] = types.NodeView{
			Path:    types.Path(v.Name),
			IsDir:   true,
			Handles: nil,
		}
		if v.IsDir {
			continue
		}
		nodes[idx].Handles = make([]types.ChunkHandle, 0)
		for i := 0; i < int(v.Chunks); i++ {
			h, err := m.cc.GetChunk(types.Path(v.Name), i)
			if err != nil {
				return err
			}
			nodes[idx].Handles = append(nodes[idx].Handles, h)
		}
	}
	argv.Root = nodes

	return nil
}
