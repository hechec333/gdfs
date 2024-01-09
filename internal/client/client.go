package client

import (
	"context"
	"fmt"
	chunkserver "gdfs/internal/chunkServer"
	"gdfs/internal/common"
	"gdfs/internal/common/rpc"
	"gdfs/types"
	"io"
	"path"
	"reflect"
	"sync"
	"time"
)

type FileFlag uint16

const (
	O_CREATE FileFlag = 1 << iota
	O_APPEND
	O_RWONLY
	O_RDONLY
)

// Client struct is the types client-side driver
type Client struct {
	mu         sync.Mutex
	clientId   int64
	seq        int64
	lastleader int
	cfg        *ClientCfg
	master     []types.Addr
	leaseBuf   *leaseBuffer
}

type ClientConfig struct {
	Master []types.Addr
}

// NewClient returns a new types client.
func NewClient(config *ClientConfig, opts ...Option) *Client {
	cli := &Client{
		master:   config.Master,
		leaseBuf: nil,
		clientId: common.Nrand(),
		cfg:      &ClientCfg{},
	}
	cli.cfg.Init(opts...)
	cli.leaseBuf = newLeaseBuffer(cli, common.LeaseBufTick)
	return cli
}
func (c *Client) Do(ctx context.Context, service string, arg, reply any) error {
	ch := make(chan error, 1)
	go func() {
		ch <- c.do(service, arg, reply)
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-ch:
		return err
	}
}
func (c *Client) do(service string, arg any, reply any) error {
	c.mu.Lock()
	seq := c.seq + 1
	c.mu.Unlock()
	rcv := reflect.ValueOf(arg)
	if rcv.Kind() == reflect.Ptr {
		rcv = rcv.Elem()
	}

	f := rcv.FieldByName("ClientIdentity")
	if f.IsValid() && f.CanSet() {
		f.Set(reflect.ValueOf(types.ClientIdentity{
			ClientId: c.clientId,
			Seq:      seq,
		}))
	}

	next := c.lastleader
	times := 0
	for {
		server := c.master[next]
		err := rpc.Call(server, service, arg, reply)
		if types.ErrEqual(err, types.ErrDuplicate) {
			return types.ErrDuplicate
		}
		// fmt.Println(types.ErrEqual(err, types.ErrRedirect))
		if types.ErrEqual(err, types.ErrTimeOut) || types.ErrEqual(err, types.ErrRedirect) {
			next = (next + 1) % len(c.master)
			if next == c.lastleader {
				time.Sleep(common.ClientRoundStrip)
			}
			times++
			if times >= common.MaxClientRetry*len(c.master) {
				common.LInfo("%v", times)
				return types.ErrRetryOverSeed
			}
			common.LInfo("<Client> redirect to next master next %v", next)
			continue
		}
		if err != nil {
			return err
		}
		c.mu.Lock()
		defer c.mu.Unlock()

		c.lastleader = next
		c.seq = seq
		return nil
	}
}

// Create is a client API, creates a file
func (c *Client) Create(path types.Path, mode FileMode) (*File, error) {
	var reply types.CreateFileReply
	arg := types.CreateFileArg{
		ClientIdentity: types.ClientIdentity{},
		Path:           path,
	}
	err := c.do("Master.RPCCreateFile", &arg, &reply)

	return NewFile(c, path, 0), err
}

func validPerm(clientperm uint8, fileperm types.FilePerm) bool {

	// 去除O_CREATE标志
	clientperm ^= uint8(O_CREATE)
	// 如果以读写的方式打开,检测文件本身属性是否可写
	if clientperm&uint8(O_RWONLY) == uint8(O_RWONLY) {
		if fileperm > types.PermReadWrite {
			return false
		}
	}
	return true
}

func (c *Client) OpenFile(path types.Path, smode FileFlag) (*File, error) {
	var (
		f   *File
		err error
	)
	mode := uint8(smode)
	arg := types.PathExistArg{
		Path: path,
		Dir:  false,
	}
	reply := types.PathExistReply{}

	err = c.do("Master.RPCPathExist", &arg, &reply)

	if types.ErrEqual(err, types.ErrPathNotFound) && mode&uint8(O_CREATE) == uint8(O_CREATE) {
		carg := types.CreateFileArg{
			Path: path,
			Perm: types.PermReadWrite,
		}
		x := types.CreateFileReply{}

		err = c.do("Master.RPCCreateFile", &carg, &x)
	}

	if err != nil {
		return nil, err
	}

	if mode&uint8(O_CREATE) != uint8(O_CREATE) {
		arg := types.GetFilePermArg{
			Path: path,
		}

		reply := types.GetFilePermReply{}

		err = c.do("Master.RPCGetFilePerm", &arg, &reply)

		if err != nil {
			return nil, err
		}

		ok := validPerm(mode, reply.Info.Mode.Perm)

		if !ok {
			err = types.ErrPermissionDenied
		}
	}

	f = NewFile(c, path, 0)
	return f, err
}

// Delete is a client API, deletes a file
func (c *Client) Delete(path types.Path) error {
	var reply types.DeleteFileReply
	arg := types.DeleteFileArg{
		ClientIdentity: types.ClientIdentity{},
		Path:           path,
	}
	err := c.do("Master.RPCDeleteFile", &arg, &reply)
	if err != nil {
		return err
	}
	return reply.Err
}

func WithForce() types.MkdirOption {
	return func(c *types.MkdirConfig) {
		c.Recursive = true
	}
}

// Mkdir is a client API, makes a directory
func (c *Client) Mkdir(path types.Path, opt ...types.MkdirOption) error {
	var cfg types.MkdirConfig
	for _, v := range opt {
		v(&cfg)
	}
	var reply types.MkdirReply
	arg := types.MkdirArg{
		Cfg:  cfg,
		Path: path,
	}
	err := c.do("Master.RPCMkdir", &arg, &reply)
	return err
}

// List is a client API, lists all files in specific directory
func (c *Client) List(path types.Path) ([]types.PathInfo, error) {
	var reply types.ListReply
	arg := types.ListArg{
		Path: path,
	}
	err := c.do("Master.RPCList", &arg, &reply)
	if err != nil {
		return nil, err
	}
	return reply.Files, reply.Err
}

// Read is a client API, read file at specific offset
// it reads up to len(data) bytes form the File. it return the number of bytes and an error.
// the error is set to io.EOF if stream meets the end of file
func (c *Client) Read(path types.Path, offset int64, data []byte) (n int, err error) {
	var f types.GetFileInfoReply

	//获取文件信息
	err = c.do("Master.RPCGetFileInfo", &types.GetFileInfoArg{Path: path}, &f)
	if err != nil {
		return -1, err
	}

	if int64(offset/common.MaxChunkSize) > f.Chunks {
		return -1, fmt.Errorf("read offset exceeds file size")
	}

	pos := 0
	for pos < len(data) {
		index := offset / common.MaxChunkSize
		chunkOffset := offset % common.MaxChunkSize

		if int64(index) >= f.Chunks {
			err = io.EOF
			break
		}

		var handle types.ChunkHandle
		handle, err = c.GetChunkHandle(path, int(index))
		if err != nil {
			return
		}
		// 数据传输开始
		if c.cfg.trace.DataTransferStart != nil {
			c.cfg.trace.DataTransferStart(index, int64(handle))
		}
		var n int
		retry := uint8(0)
		for retry <= c.cfg.retry {
			n, err = c.ReadChunk(handle, chunkOffset, data[pos:])
			if err == nil || err == io.EOF {
				break
			}
			// 保持重试
			common.LWarn("read %v connection error %v", handle, err)
			// 重试行为
			if c.cfg.trace.Retry != nil {
				c.cfg.trace.Retry(retry)
			}
			retry++
		}

		offset += int64(n)
		pos += n
		// 数据传输结束
		if c.cfg.trace.ChunkTransferDone != nil {
			c.cfg.trace.ChunkTransferDone(index, int64(handle))
		}
		if err != nil {
			break
		}
	}

	return pos, err
}

// Write is a client API. write data to file at specific offset
func (c *Client) Write(path types.Path, offset int64, data []byte) (int, error) {
	var f types.GetFileInfoReply
	arg := &types.GetFileInfoArg{
		Path: path,
	}
	err := c.do("Master.RPCGetFileInfo", arg, &f)
	if err != nil {
		return -1, err
	}

	if int64(offset/common.MaxChunkSize) > f.Chunks {
		return -1, fmt.Errorf("write offset exceeds file size")
	}

	begin := 0
	for {
		index := offset / common.MaxChunkSize
		chunkOffset := offset % common.MaxChunkSize

		handle, err := c.GetChunkHandle(path, int(index))
		if err != nil {
			return -1, err
		}

		writeMax := int(common.MaxChunkSize - chunkOffset)
		var writeLen int // 本次准备写入chunk的长度
		if begin+writeMax > len(data) {
			writeLen = len(data) - begin
		} else {
			writeLen = writeMax
		}

		for {
			err = c.WriteChunk(handle, chunkOffset, data[begin:begin+writeLen])
			if err == nil {
				break
			}
		}
		if err != nil {
			return -1, err
		}

		offset += int64(writeLen)
		begin += writeLen

		if begin == len(data) {
			break
		}
	}

	return begin, nil
}

// Append is a client API, append data to file
func (c *Client) Append(path types.Path, data []byte) (offset int64, err error) {
	if len(data) > int(common.MaxAppendSize) {
		return 0, fmt.Errorf("len(data) = %v > max append size %v", len(data), common.MaxAppendSize)
	}

	var f types.GetFileInfoReply
	arg := &types.GetFileInfoArg{
		Path: path,
	}
	err = c.do("Master.RPCGetFileInfo", &arg, &f)
	if err != nil {
		return
	}

	start := int(f.Chunks - 1)
	if start < 0 {
		start = 0
	}

	var chunkOffset int64
	for {
		var handle types.ChunkHandle
		handle, err = c.GetChunkHandle(path, start)
		if err != nil {
			return
		}
		for {
			chunkOffset, err = c.AppendChunk(handle, data)
			if err == nil || err == types.ErrAppendExceed {
				break
			}
			// log.Warning("Append ", handle, " connection error, try again ", err)
			time.Sleep(50 * time.Millisecond)
		}
		if err == nil || err == types.ErrAppendExceed {
			break
		}

		// retry in next chunk
		start++
		//log.Info("pad this, try on next chunk ", start)
	}

	if err != nil {
		return
	}

	offset = int64(start)*common.MaxChunkSize + chunkOffset
	return
}

func (c *Client) GetChunkHandle(path types.Path, index int) (types.ChunkHandle, error) {
	var reply types.GetChunkHandleReply
	arg := types.GetChunkHandleArg{
		Path:  path,
		Index: index,
	}
	err := c.do("Master.RPCGetChunkHandle", &arg, &reply)
	if err != nil {
		return 0, err
	}
	return reply.Handle, reply.Err
}

func (c *Client) ReadChunk(handle types.ChunkHandle, offset int64, data []byte) (int, error) {
	var readLen int

	if common.MaxChunkSize-offset > int64(len(data)) {
		readLen = len(data)
	} else {
		readLen = int(common.MaxChunkSize - offset)
	}

	ends, err := c.leaseBuf.GetEndpoint(handle)
	if err != nil {
		return -1, err
	}
	if len(ends) == 0 {
		return 0, types.ErrOutOfReplicas
	}
	// 分片负载均衡开始
	if c.cfg.trace.PickStart != nil {
		c.cfg.trace.PickStart(ends)
	}

	loc := c.cfg.lb.Pick(ends)
	// 分片负载均衡结束
	if c.cfg.trace.PickEndpointDone != nil {
		c.cfg.trace.PickEndpointDone(loc)
	}

	r := types.ReadChunkReply{}
	argz := types.ReadChunkArg{
		Handle: handle,
		Offset: int(offset),
		Length: readLen,
	}
	r.Data = data
	err = rpc.Call(loc.Addr, "ChunkServer.RPCReadChunk", &argz, &r)
	if err != nil {
		if types.ErrEqual(err, io.EOF) {
			err = io.EOF
		}
	}
	if r.Err != nil {
		if types.ErrEqual(r.Err, io.EOF) {
			err = io.EOF
		}
	}
	return r.Length, err
}

func (c *Client) WriteChunk(handle types.ChunkHandle, offset int64, data []byte) error {
	if int64(len(data))+int64(offset) > common.MaxChunkSize {
		return fmt.Errorf("len(data)+offset = %v > max chunk size %v", len(data)+int(offset), common.MaxChunkSize)
	}

	l, err := c.leaseBuf.Get(handle)
	if err != nil {
		return err
	}

	dataID := chunkserver.NewDataID(handle)
	chain := append(l.Backups, l.Primary)

	var d types.ForwardDataReply
	fdArg := &types.ForwardDataArg{
		DataID:     dataID,
		Data:       data,
		ChainOrder: chain[1:],
	}
	err = rpc.Call(chain[0], "ChunkServer.RPCForwardData", &fdArg, &d)
	if err != nil {
		return err
	}

	wcargs := types.WriteChunkArg{
		DataID:      dataID,
		Offset:      offset,
		Secondaries: l.Backups,
	}
	err = rpc.Call(l.Primary, "ChunkServer.RPCWriteChunk", wcargs, &types.WriteChunkReply{})
	return err
}

func (c *Client) AppendChunk(handle types.ChunkHandle, data []byte) (offset int64, err error) {
	if len(data) > int(common.MaxAppendSize) {
		return 0, fmt.Errorf("len(data) = %v > max append size %v", len(data), common.MaxAppendSize)
	}

	//log.Infof("Client : get lease ")

	l, err := c.leaseBuf.Get(handle)
	if err != nil {
		return -1, err
	}

	dataID := chunkserver.NewDataID(handle)
	chain := append(l.Backups, l.Primary)

	//log.Warning("Client : get locations %v", chain)
	var d types.ForwardDataReply
	fdArg := types.ForwardDataArg{
		DataID:     dataID,
		Data:       data,
		ChainOrder: chain[1:],
	}

	err = rpc.Call(chain[0], "ChunkServer.RPCForwardData", &fdArg, &d)
	if err != nil {
		return -1, err
	}

	//log.Warning("Client : send append request to primary. data : %v", dataID)

	var a types.AppendChunkReply
	acargs := types.AppendChunkArg{
		DataID:      dataID,
		Secondaries: l.Backups,
	}
	err = rpc.Call(l.Primary, "ChunkServer.RPCAppendChunk", acargs, &a)
	if err != nil {
		return -1, err
	}

	return a.Offset, a.Err
}

func (c *Client) Walk(xpath types.Path, fn func(p types.Path)) error {
	arg := types.SnapViewArg{}
	argv := types.SnapViewReply{}

	if xpath == "/" {
		arg.Path = "/"
	} else {
		arg.Path = types.Path(path.Join(string(xpath), "x"))
	}
	err := c.do("Master.RPCSnapView", arg, &argv)

	if err != nil {
		return err
	}

	for _, v := range argv.Root {
		if fn != nil {
			fn(v.Path)
		}
	}
	return nil
}
