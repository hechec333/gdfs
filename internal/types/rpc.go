package types

import (
	"time"
)

type ClientIdentity struct {
	ClientId int64
	Seq      int64
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
	Chunks []PersiteChunkInfo
}

type ForwardDataArg struct {
	DataID     DataBufferID
	Data       []byte
	ChainOrder []Addr
}
type ForwardDataReply struct {
	ErrorCode string
}

type ReadChunkArg struct {
	Handle ChunkHandle
	Offset int
	Length int
}
type ReadChunkReply struct {
	Data   []byte
	Length int
	Err    error
}

type WriteChunkArg struct {
	DataID      DataBufferID
	Offset      int64
	Secondaries []Addr
}
type WriteChunkReply struct {
	ErrorCode string
}

type AppendChunkArg struct {
	DataID      DataBufferID
	Secondaries []Addr
}
type AppendChunkReply struct {
	Offset int64
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
type GetPrimaryAndSecondariesReply struct {
	Primary     Addr
	Expire      time.Time
	Secondaries []Addr
	Err         error
}

type GetReplicasArg struct {
	Handle ChunkHandle
}
type GetReplicasReply struct {
	Locations []Addr
}

type GetFileInfoArg struct {
	Path Path
}
type GetFileInfoReply struct {
	Length int64
	Chunks int64
}

type GetChunkHandleArg struct {
	Path     Path
	Index    int
	ClientId int64
	Seq      int64
}
type GetChunkHandleReply struct {
	Handle ChunkHandle
	Err    error
}

// namespace operation
type CreateFileArg struct {
	ClientIdentity
	Path Path
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
