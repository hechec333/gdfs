package lb

import "gdfs/types"

type RoundRobinLBPicker struct {
	next int
}

func (rrlb *RoundRobinLBPicker) Pick(endpoints []types.EndpointInfo) types.Addr {
	x := len(endpoints)

	if rrlb.next > x {
		rrlb.next = 0
	}

	idx := rrlb.next
	rrlb.next++

	return endpoints[idx].Addr
}
