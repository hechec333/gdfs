package common

import (
	"crypto/md5"
	"hash"
	"hash/crc32"
	"io"
)

type IntergerSumer interface {
	GetSum() int64
}

type StringSumer interface {
	GetSum() string
}
type IoIntergerSumer interface {
	io.Writer
	IntergerSumer
}

type IoStringSumer interface {
	io.Writer
	StringSumer
}



type Md5Sumer struct {
	h hash.Hash
	w io.Writer
}

func NewMd5Sumer(w io.Writer) *Md5Sumer {
	return &Md5Sumer{
		w: w,
		h: md5.New(),
	}
}

func (m5 *Md5Sumer) GetSum() string {
	return string(m5.h.Sum(nil))
}

func (m5 *Md5Sumer) Write(p []byte) (n int, err error) {
	m5.h.Write(p)
	return m5.w.Write(p)
}

type Crc32Sumer struct {
	h hash.Hash32
	w io.Writer
}

func NewCrc32Sumer(w io.Writer) *Crc32Sumer {
	return &Crc32Sumer{
		w: w,
		h: crc32.NewIEEE(),
	}
}

func (c32 *Crc32Sumer) GetSum() int64 {
	return int64(c32.h.Sum32())
}

func (c32 *Crc32Sumer) Write(p []byte) (n int, err error) {
	c32.h.Write(p)
	return c32.w.Write(p)
}
