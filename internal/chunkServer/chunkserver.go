package chunkserver

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"gdfs/internal/common"
	xrpc "gdfs/internal/common/rpc"
	"gdfs/proto"
	"gdfs/types"
	"io"
	"log"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"path"
	"strings"
	"sync"
	"syscall"
	"time"
)

type ChunkServer struct {
	proto.UnimplementedChunkServerServer
	lock                   sync.RWMutex
	address                types.Addr   // chunkserver address
	masters                []types.Addr // master address
	who                    int
	spin                   int // total spin times
	lastspin               time.Time
	spinidle               time.Duration // total time cost in spin to check master
	rootDir                string        // path to data storage
	l                      net.Listener
	shutdown               chan struct{}
	property               types.ServerProperty
	dl                     *CacheBuffer                     // expiring download buffer
	chunk                  map[types.ChunkHandle]*chunkInfo // chunk information
	dead                   bool                             // set to ture if server is shuntdown
	pendingLeaseExtensions []types.ChunkHandle              // pending lease extension
	garbage                []types.ChunkHandle              // garbages
	clientid               int64
	seq                    int64
	mu                     sync.Mutex
}

type Mutation struct {
	mtype  types.MutationType
	data   []byte
	offset int64
}

type chunkInfo struct {
	sync.RWMutex
	length    int
	version   int // version number of the chunk in disk
	checksum  int
	mutations map[int64]*Mutation // mutation buffer
	abandoned bool                // unrecoverable error
}

const (
	MetaFileName = "gdfs-server.meta"
	FilePerm     = 0644
)

var ErrNotFoundMaster = errors.New("not found master")

func IsRemoteServerExitError(err error) bool {
	s := err.Error()
	if strings.Contains(s, "timeout") || strings.Contains(s, "shutdown") {
		return true
	}
	return false
}

func MustNewAndServe(config *types.ChunkServerServeConfig) *ChunkServer {
	// 1.解析配置
	// 2.配置rpc(http)
	// 3.加载元数据
	// 4.启动rpc服务
	// 5.启动背景任务

	// 1
	if config == nil {
		panic("empty serve configuartion!")
	}
	cid := common.Nrand()
	cs := &ChunkServer{
		address:  config.Address,
		masters:  config.MetaServers,
		who:      int(cid) % len(config.MetaServers),
		rootDir:  string(config.RootDir),
		shutdown: make(chan struct{}),
		dl:       newCacheBuffer(common.CacheBufferExpire, common.CacheBufferTick),
		dead:     false,
		property: types.ServerProperty{Property: config.Pro},
		garbage:  make([]types.ChunkHandle, 0),
		chunk:    make(map[types.ChunkHandle]*chunkInfo),
		seq:      0,
		clientid: cid,
	}

	common.LInfo("starting chunkserver with masteraddrs %v who %v", cs.masters, cs.who)
	// 2
	server := rpc.NewServer()
	server.Register(cs)
	l, err := net.Listen("tcp", string(cs.address))
	if err != nil {
		panic(err)
	}
	cs.l = l
	// 3
	_, err = os.Stat(cs.rootDir)
	if err != nil { // not exist
		err := os.Mkdir(cs.rootDir, FilePerm)
		if err != nil {
			panic(err)
		}
	}
	err = cs.loadMetaData()
	if err != nil {
		panic(err)
	}
	// 4
	go xrpc.NewRpcAndServe(server, cs.l, cs.shutdown, xrpc.AcceptWithTimeOut(3*time.Second))
	// 5
	go cs.GoBackGroundTask()
	go cs.GoHeartbeat()
	return cs
}

// 阻塞等待信号
func (cs *ChunkServer) GraceStop() error {
	var err error

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	<-ch
	err = cs.persiteMetaData()

	cs.shutdown <- struct{}{}
	return err
}

func (cs *ChunkServer) metaName() string {
	return "ck-" + string(cs.address) + ".meta"
}
func (cs *ChunkServer) GoBackGroundTask() {
	storeTicker := time.NewTicker(12 * time.Hour)
	garbagerTicker := time.NewTicker(8 * time.Hour)
	var err error
	for {
		select {
		case <-cs.shutdown:
			return
		case <-storeTicker.C:
			err = cs.persiteMetaData()
		case <-garbagerTicker.C:
			err = cs.garbageCollect()
		}
		if err != nil {
			log.Println("[WARN] BackGroundTask error", err)
		}
	}
}
func (cs *ChunkServer) GoHeartbeat() {
	ticker := time.NewTicker(500 * time.Millisecond)
	redirect := false
	common.LInfo("heart beat to master,current master addr %v", cs.masters[cs.who])
	var err error
	for {
		select {
		case <-cs.shutdown:
			return
		case <-ticker.C:
			if !redirect {
				err, redirect = cs.heartbeat()
				if err != nil {
					common.LWarn("heartbeat master error %v", err)
					redirect = true
				}
			} else {
				common.LInfo("deteted current master lose leader,begin leader discovery!")
				cs.lastspin = time.Now()
				err = cs.discoverMaster()
				if err == ErrNotFoundMaster {
					common.LWarn("lose communication to master err %v,spining retry", err)
					redirect = true
				} else {
					redirect = false
					cs.spinidle += time.Since(cs.lastspin)
				}
				cs.spin++
			}
		}
	}
}
func (cs *ChunkServer) discoverMaster() error {
	var (
		mu       = sync.Mutex{}
		calls    = 0
		lastTerm = 0
		wait     = make(chan struct{})
	)
	for v := range cs.masters {
		go func(peer int) {
			var reply types.MasterCheckReply
			err := xrpc.Call(cs.masters[peer], "Master.RPCCheckMaster", types.MasterCheckArg{
				Server: cs.address,
			}, &reply)
			mu.Lock()
			defer mu.Unlock()
			if err == nil && reply.Master {
				if reply.Term > lastTerm {
					cs.lock.Lock()
					defer cs.lock.Unlock()
					cs.who = peer
					lastTerm = reply.Term
				}
				wait <- struct{}{}
			}
			calls++

			if err != nil {
				common.LWarn("call rpc checkmaster error %v", err)
			}
		}(v)
	}
	for {
		select {
		case <-wait:
			return nil
		default:
			if calls == len(cs.masters) {
				return ErrNotFoundMaster
			} else {
				time.Sleep(200 * time.Millisecond)
			}
		}
	}
}
func (cs *ChunkServer) heartbeat() (error, bool) {
	cs.lock.RLock()

	abandoned := []types.ChunkHandle{}
	for k, v := range cs.chunk {
		v.RLock()
		if v.abandoned {
			abandoned = append(abandoned, k)
		}
		v.RUnlock()
	}
	cs.lock.RUnlock()
	arg := types.HeartbeatArg{
		Address:          cs.address,
		AbandondedChunks: abandoned,
	}
	var reply types.HeartbeatReply
	err := xrpc.Call(cs.masters[cs.who], "Master.RPCHeartbeat", &arg, &reply)
	if err != nil {
		return err, reply.Redirect
	}

	cs.lock.Lock()
	cs.garbage = append(cs.garbage, reply.Garbage...)
	cs.lock.Unlock()

	common.LInfo("receive garbage from master %v,current %v", reply.Garbage, cs.garbage)
	return nil, reply.Redirect
}

func (cs *ChunkServer) garbageCollect() error {
	cs.lock.Lock()
	defer cs.lock.Unlock()

	for _, v := range cs.garbage {
		cs.deleteChunk(v)
	}
	cs.garbage = make([]types.ChunkHandle, 0)
	return nil
}

func (cs *ChunkServer) RPCReportCurrentMasterAddr(args *types.ReportCurrentMasterAddrArg, reply *types.ReportCurrentMasterAddrReply) error {
	cs.lock.RLock()
	defer cs.lock.RUnlock()
	reply.Master = cs.masters[cs.who]
	reply.Spincount = cs.spin
	reply.SpinIdle = cs.spinidle
	return nil
}

func (cs *ChunkServer) RPCReportSelf(args *types.ReportSelfArg, reply *types.ReportSelfReply) error {
	cs.lock.RLock()
	defer cs.lock.RUnlock()
	common.LInfo("chunkserver begin reportself state")
	chunks := []types.PersiteChunkInfo{}
	for k, v := range cs.chunk {
		chunks = append(chunks, types.PersiteChunkInfo{
			Version:     v.version,
			Length:      v.length,
			CheckSum:    v.checksum,
			ChunkHandle: k,
		})
	}
	reply.Pro = types.ServerProperty{
		Property: cs.property.Property,
	}
	reply.Chunks = chunks
	return nil
}

func (cs *ChunkServer) loadMetaData() error {
	cs.lock.Lock()
	defer cs.lock.Unlock()

	filename := path.Join(cs.rootDir, cs.metaName())
	var (
		file *os.File
		err  error
	)
	if common.IsExist(filename) {
		file, err = os.OpenFile(filename, os.O_WRONLY, FilePerm)
	} else {
		return nil
	}
	if err != nil {
		return err
	}
	defer file.Close()

	var meta []types.PersiteChunkInfo

	d := gob.NewDecoder(file)

	err = d.Decode(&meta)
	if err != nil {
		return err
	}

	for _, ck := range meta {
		cs.chunk[ck.ChunkHandle] = &chunkInfo{
			length:   ck.Length,
			version:  ck.Version,
			checksum: ck.CheckSum,
		}
	}
	return nil
}

func (cs *ChunkServer) persiteMetaData() error {
	cs.lock.RLock()
	defer cs.lock.RUnlock()

	filename := path.Join(cs.rootDir, cs.metaName())
	var (
		file *os.File
		err  error
	)
	if common.IsExist(filename) {
		err = os.Remove(filename)
	}
	if err != nil {
		return err
	}
	file, err = os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()
	var metas []types.PersiteChunkInfo
	for handle, ck := range cs.chunk {
		metas = append(metas, types.PersiteChunkInfo{
			ChunkHandle: handle,
			Length:      ck.length,
			CheckSum:    ck.checksum,
			Version:     ck.version,
		})
	}

	//log.Infof("Server %v : store metadata len: %v", cs.address, len(metas))
	common.LInfo("Server %v : store metadata len: %v", cs.address, len(metas))
	enc := gob.NewEncoder(file)
	err = enc.Encode(metas)

	return err
}

func (cs *ChunkServer) RPCCheckReplicaVersion(args types.CheckReplicaVersionArg, reply *types.CheckReplicaVersionReply) error {
	cs.lock.RLock()
	ck, ok := cs.chunk[args.Handle]
	cs.lock.RUnlock()
	if !ok || ck.abandoned {
		return fmt.Errorf("chunk %v does not exist or is abandoned", args.Handle)
	}

	ck.Lock()
	defer ck.Unlock()

	if ck.version+1 == args.Version {
		ck.version++
		reply.IsStale = false
	} else {
		common.LWarn("Server %v : stale chunk %v", cs.address, args.Handle)
		ck.abandoned = true
		reply.IsStale = true
	}
	return nil
}

func (cs *ChunkServer) RPCForwardData(args *types.ForwardDataArg, reply *types.ForwardDataReply) error {
	if _, ok := cs.dl.Get(args.DataID); ok {
		return fmt.Errorf("data %v already exists", args.DataID)
	}

	common.LInfo("Server %v : get data %v", cs.address, args.DataID)
	cs.dl.Set(args.DataID, args.Data)
	if len(args.ChainOrder) > 0 {
		next := args.ChainOrder[0]
		args.ChainOrder = args.ChainOrder[1:]
		err := xrpc.Call(next, "ChunkServer.RPCForwardData", args, reply)
		return err
	}

	return nil
}

func (cs *ChunkServer) RPCCreateChunk(args *types.CreateChunkArg, reply *types.CreateChunkReply) error {
	cs.lock.Lock()
	defer cs.lock.Unlock()
	common.LInfo("Server %v : create chunk %v", cs.address, args.Handle)
	if _, ok := cs.chunk[args.Handle]; ok {
		//log.Warning("[ignored] recreate a chunk in RPCCreateChunk")
		common.LWarn("[ignored] recreate a chunk in RPCCreateChunk %v", args.Handle)
		return nil // TODO : error handle
		//return fmt.Errorf("Chunk %v already exists", args.Handle)
	}

	cs.chunk[args.Handle] = &chunkInfo{
		length:  0,
		version: 1,
	}
	filename := path.Join(cs.rootDir, fmt.Sprintf("chunk-%v-%v.chk", cs.address, args.Handle))
	_, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	return nil
}

func (cs *ChunkServer) RPCReadChunk(args *types.ReadChunkArg, reply *types.ReadChunkReply) error {
	handle := args.Handle
	cs.lock.RLock()
	ck, ok := cs.chunk[handle]
	cs.lock.RUnlock()
	if !ok || ck.abandoned {
		return fmt.Errorf("chunk %v does not exist or is abandoned", handle)
	}

	// read from disk
	var err error
	data := make([]byte, args.Length)

	// 判断是否命中客户端缓存
	etag, _ := cs.GetEtag(args.Handle, ck)
	if args.Etag == etag && args.Etag != "" {
		return nil
	}

	ck.RLock()
	reply.Length, err = cs.readChunk(handle, args.Offset, data)
	ck.RUnlock()
	reply.Data = append(reply.Data, data...)
	reply.Err = types.NewError(err)
	return nil
}
func (cs *ChunkServer) RPCWriteChunk(args *types.WriteChunkArg, reply *types.WriteChunkReply) error {
	data, err := cs.dl.Fetch(args.DataID)
	if err != nil {
		return err
	}

	newLen := args.Offset + int64(len(data))
	if newLen > common.MaxChunkSize {
		return fmt.Errorf("writeChunk new length is too large. Size %v > MaxSize %v", len(data), int(common.MaxChunkSize))
	}

	handle := args.DataID.Handle
	cs.lock.RLock()
	ck, ok := cs.chunk[handle]
	cs.lock.RUnlock()
	if !ok || ck.abandoned {
		return fmt.Errorf("chunk %v does not exist or is abandoned", handle)
	}

	if err = func() error {
		// ck.Lock()
		//  ck.Unlock()
		mutation := &Mutation{types.MutationWrite, data, args.Offset}

		// apply to local
		wait := make(chan error, 1)
		go func() {
			wait <- cs.doMutation(handle, mutation)
		}()

		// call secondaries
		callArgs := types.ApplyMutationArg{
			Mtype:  mutation.mtype,
			DataID: args.DataID,
			Offset: args.Offset,
		}
		errs := []error{}
		errCh := make(chan error)
		for _, v := range args.Secondaries {
			go func(peer types.Addr) {
				errCh <- xrpc.Call(peer, "ChunkServer.RPCApplyMutation", &callArgs, nil)
			}(v)
		}

		for range args.Secondaries {
			if err := <-errCh; err != nil {
				errs = append(errs, err)
			}
		}
		if err != nil {
			return err
		}

		err = <-wait
		if err != nil {
			return err
		}
		return nil
	}(); err != nil {
		return err
	}

	return nil
}
func (cs *ChunkServer) RPCAppendChunk(args *types.AppendChunkArg, reply *types.AppendChunkReply) error {

	data, err := cs.dl.Fetch(args.DataID)
	if err != nil {
		return err
	}

	if len(data) > int(common.MaxAppendSize) {
		return fmt.Errorf("append data size %v excceeds max append size %v", len(data), common.MaxAppendSize)
	}

	handle := args.DataID.Handle
	cs.lock.RLock()
	ck, ok := cs.chunk[handle]
	cs.lock.RUnlock()
	if !ok || ck.abandoned {
		return fmt.Errorf("chunk %v does not exist or is abandoned", handle)
	}

	var mtype types.MutationType

	if err = func() error {
		ck.Lock()
		defer ck.Unlock()
		newLen := ck.length + len(data)
		offset := int64(ck.length)
		if newLen > int(common.MaxChunkSize) {
			mtype = types.MutationPad
			ck.length = int(common.MaxChunkSize)
			reply.Err = types.NewError(types.ErrAppendExceed)
		} else {
			mtype = types.MutationAppend
			ck.length = newLen
		}
		reply.Offset = offset

		mutation := &Mutation{mtype, data, offset}

		//log.Infof("Primary %v : append chunk %v version %v", cs.address, args.DataID.Handle, version)
		common.LInfo("Primary %v : append chunk %v", cs.address, args.DataID.Handle, cs.chunk[args.DataID.Handle].version)
		// apply to local
		wait := make(chan error, 1)
		go func() {
			wait <- cs.doMutation(handle, mutation)
		}()

		// call secondaries
		callArgs := types.ApplyMutationArg{
			Mtype:  mtype,
			DataID: args.DataID,
			Offset: offset,
		}
		errs := []error{}
		errCh := make(chan error)
		for _, v := range args.Secondaries {
			go func(peer types.Addr) {
				errCh <- xrpc.Call(peer, "ChunkServer.RPCApplyMutation", &callArgs, nil)
			}(v)
		}

		for range args.Secondaries {
			if err := <-errCh; err != nil {
				errs = append(errs, err)
			}
		}
		if err != nil {
			return common.JoinErrors(errs...)
		}

		err = <-wait
		if err != nil {
			return err
		}
		return nil
	}(); err != nil {
		return err
	}

	return nil
}

func (cs *ChunkServer) RPCApplyMutation(args *types.ApplyMutationArg, reply *types.ApplyMutationReply) error {
	data, err := cs.dl.Fetch(args.DataID)
	if err != nil {
		return err
	}

	handle := args.DataID.Handle
	cs.lock.RLock()
	ck, ok := cs.chunk[handle]
	cs.lock.RUnlock()
	if !ok || ck.abandoned {
		return fmt.Errorf("cannot find chunk %v", handle)
	}

	//log.Infof("Server %v : get mutation to chunk %v version %v", cs.address, handle, args.Version)
	common.LInfo("Server %v : get mutation to chunk %v ", cs.address, handle)
	mutation := &Mutation{
		mtype:  args.Mtype,
		data:   data,
		offset: args.Offset,
	}
	err = func() error {
		ck.Lock()
		defer ck.Unlock()
		err = cs.doMutation(handle, mutation)
		return err
	}()

	return err
}

func (cs *ChunkServer) RPCSendCopy(args *types.SendCopyArg, reply *types.SendCopyReply) error {
	handle := args.Handle
	cs.lock.RLock()
	ck, ok := cs.chunk[handle]
	cs.lock.RUnlock()
	if !ok || ck.abandoned {
		return fmt.Errorf("chunk %v does not exist or is abandoned", handle)
	}

	ck.RLock()
	defer ck.RUnlock()

	common.LInfo("Server %v : Send copy of %v to %v", cs.address, handle, args.Address)
	data := make([]byte, ck.length)
	_, err := cs.readChunk(handle, 0, data)
	if err != nil {
		return err
	}
	arg := types.ApplyCopyArg{
		Handle:  handle,
		Data:    data,
		Version: ck.version,
	}
	var r types.ApplyCopyReply
	err = xrpc.Call(args.Address, "ChunkServer.RPCApplyCopy", &arg, &r)
	if err != nil {
		return err
	}

	return nil
}

// 获取指定chunk的etag
// etag包含chunk检验和，chunk最后更改时间
func (cs *ChunkServer) GetEtag(handle types.ChunkHandle, cki *chunkInfo) (string, error) {

	filename := fmt.Sprintf("chunk-%v-%v.chk", cs.address, handle)

	f, err := os.Open(filename)

	if err != nil {
		return "", err
	}

	st, err := f.Stat()

	if err != nil {
		return "", err
	}

	etag := common.GetEtag(cki.checksum, st.ModTime().Unix())

	return etag, err
}

func (cs *ChunkServer) RPCApplyCopy(args *types.ApplyCopyArg, reply *types.ApplyCopyReply) error {
	handle := args.Handle
	cs.lock.RLock()
	ck, ok := cs.chunk[handle]
	cs.lock.RUnlock()
	if !ok || ck.abandoned {
		return fmt.Errorf("chunk %v does not exist or is abandoned", handle)
	}

	ck.Lock()
	defer ck.Unlock()

	common.LInfo("Server %v : Apply copy of %v", cs.address, handle)
	ck.version = args.Version
	err := cs.writeChunk(handle, args.Data, 0, true)
	if err != nil {
		return err
	}
	return nil
}

func (cs *ChunkServer) writeChunk(handle types.ChunkHandle, data []byte, offset int64, lock bool) error {
	cs.lock.RLock()
	ck := cs.chunk[handle]
	cs.lock.RUnlock()

	// ck is already locked in top caller
	newLen := int(offset) + len(data)
	if newLen > ck.length {
		ck.length = newLen
	}

	if newLen > int(common.MaxChunkSize) {
		log.Println(("new length > types.MaxChunkSize"))
		return fmt.Errorf("length oversize")
	}

	//log.Infof("Server %v : write to chunk %v at %v len %v", cs.address, handle, offset, len(data))
	common.LInfo("Server %v : write to chunk %v at %v len %v", cs.address, handle, offset, len(data))
	filename := path.Join(cs.rootDir, fmt.Sprintf("chunk-%v-%v.chk", cs.address, handle))
	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, FilePerm)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.WriteAt(data, int64(offset))
	if err != nil {
		return err
	}
	buf := new(bytes.Buffer)
	io.Copy(buf, file)
	ck.checksum = int(common.GetCrc32(buf.Bytes()))
	return nil
}

// readChunk reads data at offset from a chunk at dist
func (cs *ChunkServer) readChunk(handle types.ChunkHandle, offset int, data []byte) (int, error) {
	filename := path.Join(cs.rootDir, fmt.Sprintf("chunk-%v-%v.chk", cs.address, handle))

	f, err := os.Open(filename)
	if err != nil {
		return -1, err
	}
	defer f.Close()
	common.LTrace("Server %v : read chunk %v at %v len %v", cs.address, handle, offset, len(data))
	//log.Infof("Server %v : read chunk %v at %v len %v", cs.address, handle, offset, len(data))
	return f.ReadAt(data, int64(offset))
}

// deleteChunk deletes a chunk during garbage collection
func (cs *ChunkServer) deleteChunk(handle types.ChunkHandle) error {
	cs.lock.Lock()
	delete(cs.chunk, handle)
	cs.lock.Unlock()

	filename := path.Join(cs.rootDir, fmt.Sprintf("chunk-%v-%v.chk", cs.address, handle))
	err := os.Remove(filename)
	return err
}

// apply mutations (write, append, pad) in chunk buffer in proper order according to version number
func (cs *ChunkServer) doMutation(handle types.ChunkHandle, m *Mutation) error {
	// already locked
	var lock bool
	if m.mtype == types.MutationAppend {
		lock = true
	} else {
		lock = false
	}

	var err error
	if m.mtype == types.MutationPad {
		data := []byte{0} //?
		err = cs.writeChunk(handle, data, common.MaxChunkSize-1, lock)
	} else {
		err = cs.writeChunk(handle, m.data, m.offset, lock)
	}

	if err != nil {
		cs.lock.RLock()
		ck := cs.chunk[handle]
		cs.lock.RUnlock()
		//log.Warningf("%v abandon chunk %v", cs.address, handle)
		common.LWarn("%v abandon chunk %v", cs.address, handle)
		ck.abandoned = true
		return err
	}

	return nil
}

// grpc implentments

func (cs *ChunkServer) GRPCReadChunk(ctx context.Context, req *proto.ReadChunkArg) (*proto.ReadChunkReply, error) {
	arg := types.ReadChunkArg{
		Handle: types.ChunkHandle(req.Handle),
		Offset: int(req.Offset),
		Length: int(req.Length),
	}
	var reply types.ReadChunkReply
	err := cs.RPCReadChunk(&arg, &reply)

	return &proto.ReadChunkReply{
		Data:   reply.Data,
		Length: req.Length,
	}, err
}

func (cs *ChunkServer) GRPCWriteChunk() {

}

func (cs *ChunkServer) GRPCAppendChunk() {

}
