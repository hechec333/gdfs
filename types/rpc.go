package types

import (
	"time"
)

type ClientIdentity struct {
	ClientId int64
	Seq      int64
}

type GetFileDetailArg struct {
	Path Path
}
type ChunkDetail struct {
	Handle  ChunkHandle
	Lease   LeaseInfo
	Version int64
}
type GetFileDetailReply struct {
	Length int64
	Chunks []ChunkDetail
}

type BatchOpArg struct {
	ClientIdentity
	Method string
	Cmd    []interface{}
	Arg    []interface{}
}

type BatchOpReply struct {
	Err    error
	Status []interface{}
}

type GetFilePermArg struct {
	Path Path
}

type GetFilePermReply struct {
	Info FileInfo
}

type CreateChunkArg struct {
	Handle ChunkHandle
}

type CreateChunkReply struct {
}

type CheckReplicaVersionArg struct {
	Handle  ChunkHandle
	Version int
}

type CheckReplicaVersionReply struct {
	IsStale bool
}
type HeartbeatArg struct {
	Address          Addr          // chunkserver address
	AbandondedChunks []ChunkHandle // unrecoverable chunks
}
type HeartbeatReply struct {
	Redirect bool
	Garbage  []ChunkHandle
}

type ReportSelfArg struct {
}

type ReportSelfReply struct {
	Pro    ServerProperty
	Chunks []PersiteChunkInfo
}
type ForwardOption struct {
	Sync            bool // 是否同步调用
	AtLeastResponce int  // 最少同步等待结点数
	Wait            int  // 最大同步等待时间,秒
}
type ForwardDataArg struct {
	ForwardOption
	DataID     DataBufferID
	Data       []byte
	ChainOrder []Addr
}
type ForwardDataReply struct {
	RepsonceNode int // 成功复制的结点数
}

// chunkserver
type ReadChunkArg struct {
	Handle ChunkHandle
	Offset int
	Length int
	Etag   string
}
type ReadChunkReply struct {
	Data   []byte
	Length int
	Err    error
	Etag   string
}
type WriteChunkArg struct {
	DataID      DataBufferID
	Offset      int64
	Secondaries []Addr
}
type WriteChunkReply struct {
	ErrorCode string
	Etag      string
}

type AppendChunkArg struct {
	DataID      DataBufferID
	Secondaries []Addr
}
type AppendChunkReply struct {
	Offset int64
	Etag   string
	// ErrorCode string
	Err error
}

type SendCopyArg struct {
	Handle  ChunkHandle
	Address Addr
}
type SendCopyReply struct {
	ErrorCode string
}

type ApplyCopyArg struct {
	Handle  ChunkHandle
	Data    []byte
	Version int
}
type ApplyCopyReply struct {
	ErrorCode string
}

type ApplyMutationArg struct {
	Mtype  MutationType
	DataID DataBufferID
	Offset int64
}
type ApplyMutationReply struct {
	ErrorCode string
}

type GetPrimaryAndSecondariesArg struct {
	ClientIdentity
	Handle ChunkHandle
}

type EndpointInfo struct {
	Addr     Addr
	Property ServerProperty
}

type GetPrimaryAndSecondariesReply struct {
	Primary     EndpointInfo
	Expire      time.Time
	Secondaries []EndpointInfo
	Err         error
}

type GetReplicasArg struct {
	Handle ChunkHandle
}
type GetReplicasReply struct {
	Locations []EndpointInfo
}

type GetFileInfoArg struct {
	ClientIdentity
	Path Path
}
type GetFileInfoReply struct {
	Length int64
	Chunks int64
}

type GetChunkHandleArg struct {
	ClientIdentity
	Path     Path
	Index    int
	ClientId int64
	Seq      int64
}
type GetChunkHandleReply struct {
	Handle ChunkHandle
	Err    error
}

type SetFilePermArg struct {
	ClientIdentity
	Path Path
	Perm FilePerm
}

type SetFilePermReply struct{}

// namespace operation
type CreateFileArg struct {
	ClientIdentity
	Path Path
	Perm FilePerm
}
type CreateFileReply struct {
}

type DeleteFileArg struct {
	ClientIdentity
	Path Path
}
type DeleteFileReply struct {
	Err error
}

type RenameFileArg struct {
	Source Path
	Target Path
}
type RenameFileReply struct{}

type MkdirConfig struct {
	Recursive bool
}
type MkdirOption func(*MkdirConfig)

type MkdirArg struct {
	ClientIdentity
	Cfg       MkdirConfig
	Path      Path
	Recursive bool
}
type MkdirReply struct {
}

type ListArg struct {
	Path Path
}
type ListReply struct {
	Err   error
	Files []PathInfo
}

//master

type MasterCheckArg struct {
	Server Addr
}

type MasterCheckReply struct {
	Server Addr
	Master bool
	Term   int
}

type ReportCurrentMasterAddrArg struct {
}

type ReportCurrentMasterAddrReply struct {
	Master    Addr
	SpinIdle  time.Duration
	Spincount int
}

type SnapViewArg struct {
	Path Path
}

type SnapViewReply struct {
	Root []NodeView
}

type PathExistArg struct {
	Path Path
	Dir  bool
}

type PathExistReply struct {
	Ok bool
}
