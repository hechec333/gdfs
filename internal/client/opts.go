package client

import "gdfs/internal/common"

type Option func(*ClientCfg)

func WithLoadBalancer(name string) Option {
	return func(cc *ClientCfg) {
		llb := lb.use(name)
		if llb != nil {
			cc.lb = llb
		} else {
			cc.lb = lb.use(LBdefaultTag)
		}
	}
}

func WithTrace(t *Trace) Option {
	return func(cc *ClientCfg) {
		cc.trace = t
	}
}

func WithRetry(count uint8) Option {
	return func(cc *ClientCfg) {
		if count < common.ClientLBRetry {
			count = common.ClientLBRetry
		}
		cc.retry = count
	}
}

type ClientCfg struct {
	// 访问chunkserver的轮询策略
	lb LoadBlancer

	// 访问chunkserver失败时的重试行为
	retry uint8

	// 跟踪和chunkserver发送/接受数据的回调地址
	trace *Trace
}

func (cfg *ClientCfg) Init(opts ...Option) {
	cfg.defaultCfg()

	for _, opt := range opts {
		opt(cfg)
	}
}

func (cfg *ClientCfg) defaultCfg() {
	cfg.lb = lb.use(LBdefaultTag)
	cfg.retry = common.ClientLBRetry

	if common.ClientTraceEnable {
		cfg.trace = defaultTrace
	}
}
