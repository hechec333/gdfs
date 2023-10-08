package types

import "time"

var LOG_PATH = ".log"

type ChunkHandle int64
type Path string
type Addr string

type ChunkServerServeConfig struct {
	Address     Addr
	MetaServers []Addr
	RootDir     Path
}

type MetaServerServeConfig struct {
	Me       int
	Servers  []Addr
	Protocol []Addr
}

type PathInfo struct {
	Path   Path
	IsDir  bool
	Chunks int64
	Length int64
}

type FileInfo struct {
	Path   Path
	Chunks int64
	Length int64
}

type LeaseInfo struct {
	Primary Addr
	Backups []Addr
	Exipre  time.Time
}
type PersiteFileInfo struct {
	Path     Path
	RefCount int
	Chunk    map[int]PersiteChunkInfo
}
type PersiteChunkControlor struct {
	Files []PersiteFileInfo
}
type PersiteTreeNode struct {
	IsDir    bool
	Name     string
	Children []int
	Length   int64
	Chunks   int64
}
type PersiteChunkInfo struct {
	Version     int
	Length      int
	CheckSum    int
	ChunkHandle ChunkHandle
}

type DataBufferID struct {
	Handle    ChunkHandle
	TimeStamp int
}

type MutationType int

const (
	MutationWrite = iota
	MutationAppend
	MutationPad
)
