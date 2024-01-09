package client

import (
	"gdfs/internal/common"
	"gdfs/types"
)

type Trace struct {
	// 开始请求
	Start func(*Client)
	// 开始和chunkserver交互数据
	StartTransfer func(int64)
	// 负载均衡开始
	PickStart func([]types.EndpointInfo)
	// 负载均衡结束
	PickEndpointDone func(types.EndpointInfo)
	// 重试
	Retry func(uint8)
	// 数据传输开始
	DataTransferStart func(int64, int64)
	// 数据传输结束
	ChunkTransferDone func(int64, int64)
	// 结束
	Done func()
}

var defaultTrace = &Trace{
	Start: func(*Client) {
		common.LTrace("client call start")
	},
	StartTransfer: func(i int64) {
		common.LTrace("client tranfer total chunk %v", i)
	},
	DataTransferStart: func(offset int64, id int64) {
		common.LTrace("transfering %v chunk %v start", offset, id)
	},
	PickStart: func(ei []types.EndpointInfo) {
		common.LTrace("loadbalance start total%v", ei)
	},
	PickEndpointDone: func(ei types.EndpointInfo) {
		common.LTrace("load balance pick %v", ei)
	},
	ChunkTransferDone: func(offset int64, id int64) {
		common.LTrace("transfering %v chunk %v done", offset, id)
	},
	Done: func() {
		common.LTrace("client call done")
	},
}
