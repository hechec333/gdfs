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
	ClientTraceEnable = false
)

var (
	ClientRoundStrip          = 50 * time.Millisecond //clinet和master一轮重试后暂停时间
	MaxClientRetry            = 2                     // client和master重试轮数
	MinChunks                 = 2                     //chunk数量阈值
	MaxRaftState              = 8092
	ExpireTimeout             = 60              // lease超时
	HeartBeatDuration         = 1 * time.Second // chunkserver心跳周期
	MaxChunkSize              = int64(64 * 1024 * 1024)
	MaxAppendSize             = MaxChunkSize / 4 // 最大可追加长度
	ReplicasLevel             = 3                // chunkserver复制等级
	SnapInterval              = 12 * time.Hour   // raft层快照后期
	LazyCollectInterval       = 5 * time.Second
	CheckInterval             = 10000 * time.Millisecond
	HeartBetaInterval         = 500 * time.Millisecond
	CacheBufferExpire         = 2 * time.Minute
	CacheBufferTick           = 30 * time.Second
	LeaseBufTick              = 500 * time.Millisecond // 客户端lease
	ProposalTimeout           = 3 * time.Minute        // raft提案超时
	LoaclReadTimeout          = 3 * time.Minute        // 等待LR超时时间
	ClientLBRetry       uint8 = 1                      // 客户端负载均衡失败重试次数
)
