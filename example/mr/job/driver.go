package mr

import (
	"gdfs/internal/client"
	"gdfs/types"
	"io"
	"os"
)

type StorageDriver interface {
	Create(string) (io.ReadWriteCloser, error)
	Exist(string) bool
	Open(string) (io.ReadWriteCloser, error)
	Remove(string) error
}

type LocalStorageDriver struct {
}

func NewLocalDriver() *LocalStorageDriver {
	return &LocalStorageDriver{}

}

func (lsd *LocalStorageDriver) Create(f string) (io.ReadWriteCloser, error) {
	return os.Create(f)
}

func (lsd *LocalStorageDriver) Exist(f string) bool {
	_, err := os.Stat(f)
	return err == nil || os.IsExist(err)
}

func (lsd *LocalStorageDriver) Open(f string) (io.ReadWriteCloser, error) {
	return os.Open(f)
}

func (lsd *LocalStorageDriver) Remove(f string) error {
	return os.Remove(f)
}

type GdfsStorageDriver struct {
	m   []types.Addr
	cli *client.Client
}

func NewGdfsStorageDriver(m []types.Addr) *GdfsStorageDriver {
	return &GdfsStorageDriver{
		m:   m,
		cli: client.NewClient(m),
	}
}

func (gsd *GdfsStorageDriver) Create(f string) (io.ReadWriteCloser, error) {
	return gsd.cli.Create(types.Path(f))
}

func (gsd *GdfsStorageDriver) Remove(f string) error {
	return gsd.cli.Delete(types.Path(f))
}

func (gsd *GdfsStorageDriver) Open(f string) (io.ReadWriteCloser, error) {
	return gsd.cli.OpenFile(types.Path(f), int32(client.GDFS_RWONLY))
}

func (gsd *GdfsStorageDriver) Exist(f string) bool {
	_, err := gsd.cli.OpenFile(types.Path(f), int32(client.GDFS_RWONLY))

	return err == nil || types.ErrEqual(err, types.ErrNotFound)
}
