package types

import (
	"encoding/gob"
	"errors"
)

const (
	// 系统错误码
	ErrTimeoutCode       = 501
	ErrRedirectCode      = 502
	ErrDuplicateCode     = 503
	ErrRetryOverSeedCode = 504
	ErrDialHupCode       = 505

	// 逻辑错误
	ErrNotExistCode         = 401
	ErrUnknownOperationCode = 402
	ErrUnknownLogCode       = 403
	ErrChunkAllLoseCode     = 404
	ErrNotFoundCode         = 405
	ErrUnReachAbleCode      = 406
	ErrPathNotFoundCode     = 407
	ErrInvalidArgumentCode  = 408
	ErrPathExistsCode       = 409
	ErrPermissionDeniedCode = 410
	// 客户端
	ErrOutOfReplicasCode = 301
	ErrAppendExceedCode  = 302
	ErrLBExistsCode      = 303
)

// sys error
var (
	ErrTimeOut       = errors.New("i/o timeout")
	ErrRedirect      = errors.New("not leader")
	ErrDuplicate     = errors.New("duplicate request")
	ErrRetryOverSeed = errors.New("too many retry")
	ErrFine          = error(nil)
)

// rpc error
var (
	ErrDialHup = errors.New("not found endpoint")
)

// logic error
var (
	ErrNotExist         = errors.New("target not existd")
	ErrUnknownOperation = errors.New("operation not supported")
	ErrUnknownLog       = errors.New("log type not supported")
	ErrChunkAllLose     = errors.New("chunk all lose")
	ErrNotFound         = errors.New("not found data")
	ErrUnReachAble      = errors.New("unreachable branch")
	ErrPathNotFound     = errors.New("path not found")
	ErrInvalidArgument  = errors.New("invalid argrument")
	ErrPathExists       = errors.New("file or folder already exist")
	ErrPermissionDenied = errors.New("file perm denied")
)

// client
var (
	ErrOutOfReplicas = errors.New("less than one replica")
	ErrAppendExceed  = errors.New("append exceed")
	ErrLBExists      = errors.New("lb already exists")
)

func init() {
	gob.Register(Error{})
}

func ErrEqual(t error, tt error) bool {
	if t == nil {
		if tt == nil {
			return true
		}
		return false
	}
	return t == tt || t.Error() == tt.Error()
}
func Errln(err error, msg string) error {
	return errors.New(err.Error() + ": " + msg)
}

type Error struct {
	Err string
}

func NewError(err error) Error {
	return Error{
		Err: err.Error(),
	}
}

func Match(err error, e Error) bool {
	return err.Error() == e.Err
}

func (e Error) Error() string {
	return e.Err
}
