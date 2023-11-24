package fs

import (
	"gdfs/internal/client"
	"gdfs/internal/types"
	"gdfs/toolkit/cli"
)

func mkdir(ctx *cli.CliContext, path types.Path, f bool) error {

	opts := make([]types.MkdirOption, 0)

	if f {
		opts = append(opts, client.WithForce())
	}

	return ctx.Client.Mkdir(path, opts...)
}
