package client

import (
	"gdfs/internal/types"
	"sync"
)

type FileMode int32

const (
	GDFS_RDONLY FileMode = iota << 1
	GDFS_RWONLY
	GDFS_APONLY
)

type File struct {
	sync.RWMutex
	c        *Client
	p        types.Path
	mode     int32
	readoff  int64
	writeoff int64
}

func (f *File) Close() error {
	// TODO: Implement this method
	return nil
}

func (f *File) Read(p []byte) (n int, err error) {
	f.RLock()
	defer f.RUnlock()
	return f.c.Read(f.p, int64(f.readoff), p)
}

func (f *File) Write(p []byte) (n int, err error) {
	f.Lock()
	defer f.RUnlock()
	if f.mode&int32(GDFS_APONLY) == int32(GDFS_APONLY) {
		off, err := f.c.Append(f.p, p)
		num := off - f.writeoff
		f.writeoff = off
		return int(num), err
	} else {
		return f.c.Write(f.p, f.writeoff, p)
	}
}

func (f *File) Seek(offset int64, whence int) (int64, error) {
	// TODO: Implement this method
	f.Lock()
	defer f.Unlock()

	return 0, nil
}
