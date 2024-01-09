package wal

import (
	"bytes"
	"encoding/gob"
	"errors"
	"gdfs/internal/common"
	"gdfs/types"
	"os"
	"sync"
	"time"
)

var (
	raftName = ".raft"
	snapName = ".snap"
)

type Persister struct {
	mu          sync.Mutex
	raftstate   []byte
	raftVersion types.Version
	snapshot    []byte
	snapVersion types.Version
	dir         string
	raftFile    *os.File
	snapFile    *os.File
}

func clone(orig []byte) []byte {
	x := make([]byte, len(orig))
	copy(x, orig)
	return x
}

func NewDefaultPersiter(dir string) types.IPersiter {
	pe := Persister{
		dir: dir,
	}
	err := pe.init()
	if err != nil {
		return nil
	}

	return &pe
}

func (ps *Persister) init() (err error) {
	rf := ps.dir + "/" + raftName
	sf := ps.dir + "/" + snapName
	if common.IsExist(rf) {
		ps.raftFile, err = os.Open(rf)
		if err != nil {
			return
		}
	} else {
		ps.raftFile, err = os.Create(rf)
		if err != nil {
			return
		}
	}

	if common.IsExist(sf) {
		ps.snapFile, err = os.Open(sf)
		if err != nil {
			return
		}
	} else {
		ps.snapFile, err = os.Create(sf)
		if err != nil {
			return
		}
	}
	ps.readRaft()
	ps.readSnap()
	return
}

func (ps *Persister) readRaft() {
	st, _ := ps.raftFile.Stat()
	if st.Size() <= 1 {
		ps.raftVersion.Version = 0
		return
	}
	d := gob.NewDecoder(ps.raftFile)
	if d.Decode(&ps.raftVersion) != nil || d.Decode(&ps.raftstate) != nil {
		panic(errors.New("readRaft decode error"))
	}
}
func (ps *Persister) syncRaft() error {
	buf := new(bytes.Buffer)
	e := gob.NewEncoder(buf)

	e.Encode(ps.raftVersion)
	e.Encode(ps.raftstate)

	_, err := ps.raftFile.WriteAt(buf.Bytes(), 0)
	ps.raftFile.Sync()
	return err
}
func (ps *Persister) readSnap() {
	st, _ := ps.snapFile.Stat()
	if st.Size() <= 1 {
		ps.snapVersion.Version = 0
		return
	}
	d := gob.NewDecoder(ps.snapFile)
	if d.Decode(&ps.snapVersion) != nil || d.Decode(&ps.snapshot) != nil {
		panic(errors.New("readSnapShot decode error"))
	}
}
func (ps *Persister) raftCheckSum() {
	ps.raftVersion.Md5 = common.GetMd5(ps.raftstate)
}

func (ps *Persister) snapCheckSum() {
	ps.snapVersion.Md5 = common.GetMd5(ps.snapshot)
}
func (ps *Persister) syncSnap() error {
	buf := new(bytes.Buffer)
	e := gob.NewEncoder(buf)

	e.Encode(ps.snapVersion)
	e.Encode(ps.snapshot)

	_, err := ps.raftFile.WriteAt(buf.Bytes(), 0)
	ps.snapFile.Sync()
	return err
}
func (ps *Persister) SaveRaftState(state []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.raftstate = clone(state)
	ps.raftVersion.LastModified = time.Now()
	ps.raftVersion.Version++
	ps.syncRaft()
}

func (ps *Persister) ReadRaftState() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return clone(ps.raftstate)
}

func (ps *Persister) RaftStateSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.raftstate)
}

func (ps *Persister) SaveStateAndSnapshot(state []byte, snapshot []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.raftVersion.LastModified = time.Now()
	ps.snapVersion.LastModified = time.Now()
	ps.raftVersion.Version++
	ps.snapVersion.Version++
	ps.raftstate = clone(state)
	ps.snapshot = clone(snapshot)
	ps.raftCheckSum()
	ps.snapCheckSum()
	ps.syncRaft()
	ps.syncSnap()
}

func (ps *Persister) ReadSnapshot() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return clone(ps.snapshot)
}

func (ps *Persister) SnapshotSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.snapshot)
}

func (ps *Persister) GetRaftStateVersion() types.Version {
	return ps.raftVersion
}

func (ps *Persister) GetSnapshotVersion() types.Version {
	return ps.snapVersion
}
