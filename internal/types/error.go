package types

import "errors"

// sys error
var (
	ErrTimeOut       = errors.New("i/o timeout")
	ErrRedirect      = errors.New("not leader")
	ErrDuplicate     = errors.New("duplicate request")
	ErrRetryOverSeed = errors.New("too many retry")
	ErrFine          = error(nil)
)

// logic error
var (
	ErrNotExist         = errors.New("target not existd")
	ErrUnknownOperation = errors.New("operation not supported")
	ErrUnknownLog       = errors.New("log type not supported")
	ErrChunkAllLose     = errors.New("chunk all lose")
	ErrNotFound         = errors.New("not found data")
)

// client
var (
	ErrOutOfReplicas = errors.New("less than one replica")
	ErrAppendExceed  = errors.New("append exceed")
)
