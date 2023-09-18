package types

import (
	"time"
)

type Version struct {
	LastModified time.Time
	Version      int64
	Md5          string
}

type IPersiter interface {
	SaveRaftState([]byte)
	ReadRaftState() []byte
	RaftStateSize() int
	SaveStateAndSnapshot([]byte, []byte)
	ReadSnapshot() []byte
	SnapshotSize() int
	GetRaftStateVersion() Version
	GetSnapshotVersion() Version
}

const (
	ChunkLog = iota
	NsLog
)

const (
	CommandCreate = iota
	CommandUpdate
	CommandDelete
)

type RedoLog struct {
	Type int
	Txn  []interface{}
}
type ChunkLogImpl struct {
	CommandType int
	Path        Path
	Chunk       PersiteChunkInfo
}

type NsLogImpl struct {
	CommandType int
	Path        Path
	File        PersiteTreeNode
}
