package client

import (
	"gdfs/types"
	"sync"
)

type LoadBlancer interface {
	Pick([]types.EndpointInfo) types.EndpointInfo
	Tag() string
}

var LBdefaultTag = "RoundRobin"

func Register(llb LoadBlancer) error {
	if lb == nil {
		lb = new(LBRegistery)
	}
	return lb.register(llb)
}

func init() {
	lb = new(LBRegistery)
}

var lb *LBRegistery

type LBRegistery struct {
	re map[string]LoadBlancer
	mu sync.Mutex
}

func (re *LBRegistery) register(llb LoadBlancer) error {
	re.mu.Lock()
	defer re.mu.Unlock()
	if re.re == nil {
		re.re = make(map[string]LoadBlancer)
	}
	if _, ok := re.re[llb.Tag()]; ok {
		return types.ErrLBExists
	}

	re.re[llb.Tag()] = llb
	return nil
}

func (re *LBRegistery) use(name string) LoadBlancer {
	re.mu.Lock()
	defer re.mu.Unlock()
	return re.re[name]
}
