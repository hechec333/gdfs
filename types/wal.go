package types

import (
	"encoding/gob"
	"time"
)

func init() {
	gob.Register(NsLogImpl{})
	gob.Register(ChunkLogImpl{})
	gob.Register(BatchLogImpl{})
}

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
	BatchLog
)

const (
	CommandCreate = (iota + 1) << 1
	CommandUpdate
	CommandDelete
)

const (
	OP_FILE = (iota + 1) << 18
	OP_DIC
)

type RedoLog struct {
	Type int
	Txn  []interface{}
}

type BatchLogImpl struct {
	Type  int
	Batch []interface{}
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
	Dics        []PersiteTreeNode
}
