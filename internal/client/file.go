package client

import (
	"gdfs/types"
	"sync"
)

type FileMode uint8

const (
	GDFS_RDONLY FileMode = iota << 1
	GDFS_RWONLY
	GDFS_APONLY
)

type File struct {
	sync.RWMutex
	c    *Client
	p    types.Path
	mode uint8
	off  int64
}

func NewFile(c *Client, path types.Path, mode FileMode) *File {
	return &File{
		c:    c,
		p:    path,
		mode: uint8(mode),
		off:  0,
	}
}

func (f *File) Close() error {
	// TODO: Implement this method
	return nil
}

func (f *File) Read(p []byte) (n int, err error) {
	f.RLock()
	defer f.RUnlock()
	return f.c.Read(f.p, int64(f.off), p)
}

func (f *File) Write(p []byte) (n int, err error) {
	f.Lock()
	defer f.Unlock()
	if f.mode&uint8(GDFS_APONLY) == uint8(GDFS_APONLY) {
		off, err := f.c.Append(f.p, p)
		num := off - f.off
		f.off = off
		return int(num), err
	} else {
		return f.c.Write(f.p, f.off, p)
	}
}

func (f *File) Seek(offset int64, whence int) (int64, error) {
	// TODO: Implement this method
	f.Lock()
	defer f.Unlock()

	return 0, nil
}
