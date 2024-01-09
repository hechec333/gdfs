package lb

import "gdfs/types"

// 平滑优先级负载均衡
type ProirLBPicker struct {
	real  map[types.Addr]int
	leave map[types.Addr]int
}

func (plb *ProirLBPicker) init(endpoints []types.EndpointInfo) {
	if plb.real == nil {
		plb.real = make(map[types.Addr]int)
	}
	var ok bool
	var prior int
	for _, v := range endpoints {
		if _, ok = plb.real[v.Addr]; !ok {
			x := v.Property.Property["priority"]
			if x == nil {
				prior = 1
			} else {
				prior = x.(int)
			}
			plb.real[v.Addr] = prior
			plb.leave[v.Addr] = prior
		}
	}
}

func (plb *ProirLBPicker) next() types.Addr {
	var min int = 0
	var addr types.Addr
	for i, v := range plb.leave {
		if min > v {
			min = v
			addr = i
		}
	}

	if min == 0 { // 一轮trip过后，重新填充leave
		for i, v := range plb.real {
			plb.leave[i] = v
		}
		return plb.next()
	}

	plb.leave[addr]--
	return addr
}
func (plb *ProirLBPicker) Pick(endpoints []types.EndpointInfo) types.Addr {

	plb.init(endpoints)

	return plb.next()
}
