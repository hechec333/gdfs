package lb

import (
	"gdfs/internal/client"
	"gdfs/types"
	"math/rand"
)

func init() {
	client.Register(&RandomLBPicker{})
}

type RandomLBPicker struct {
}

func (rlb *RandomLBPicker) Pick(endpoins []types.EndpointInfo) types.EndpointInfo {
	x := len(endpoins)
	n := rand.Intn(x)
	return endpoins[n]
}

func (rlb *RandomLBPicker) Tag() string {
	return "random"
}
