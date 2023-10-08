package common

import "time"

type LogLevel uint8

const (
	LOG_TRACE LogLevel = iota
	LOG_INFO
)

var (
	DefaultLogLevel = LOG_INFO
	// 是否启用完整日志输出
	LogCompleteEnable = true
)

var (
	MaxClientRetry      = 2
	MinChunks           = 2
	MaxRaftState        = 8092
	ExpireTimeout       = 5
	HeartBeatDuration   = 1 * time.Second
	MaxChunkSize        = int64(64 * 1024 * 1024)
	MaxAppendSize       = MaxChunkSize / 4
	ReplicasLevel       = 3
	SnapInterval        = 12 * time.Hour
	LazyCollectInterval = 5 * time.Second
	CheckInterval       = 800 * time.Millisecond
	HeartBetaInterval   = 500 * time.Millisecond
	CacheBufferExpire   = 2 * time.Minute
	CacheBufferTick     = 30 * time.Second
	LeaseBufTick        = 500 * time.Millisecond
)
