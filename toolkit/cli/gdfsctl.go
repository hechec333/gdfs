package cli

import (
	"gdfs/internal/client"
	"gdfs/internal/types"
)

type EndPointCfg struct {
	endpoint []types.Addr
}

type CliContext struct {
	EndPointCfg
	Client  client.Client
	History []string
	Pwd     types.Path
}
