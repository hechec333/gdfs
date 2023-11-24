package fs

import (
	"gdfs/internal/client"
	"gdfs/internal/types"
	"gdfs/toolkit/cli"
)

func touch(ctx *cli.CliContext, path types.Path) (*client.File, error) {
	return ctx.Client.Create(path)
}
