package fs

import (
	"gdfs/internal/client"
	"gdfs/toolkit/cli/cmd"
	"gdfs/types"

	"github.com/urfave/cli/v2"
)

func init() {
	register(&cli.Command{
		Name: "mkdir",
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:    "force",
				Aliases: []string{"f"},
				Value:   true,
			},
		},
		Action: func(ctx *cli.Context) error {
			return Mkdir(ctx)
		},
	})
}

func Mkdir(ctx *cli.Context) error {
	var force bool

	names := ctx.LocalFlagNames()

	for _, v := range names {
		if v == "force" {
			force = true
			break
		}
	}

	if ctx.NArg() < 1 {
		return ErrNotEnoughArgs
	}

	cc := cmd.NewCliContext(cmd.EndPointCfg{
		Endpoint: cmd.GetMasters(ctx),
	})

	var (
		terr error
	)

	for idx := 0; idx < ctx.NArg(); idx++ {
		err := mkdir(cc, types.Path(ctx.Args().Get(idx)), force)
		if err != nil {
			terr = cmd.AppendError(terr, err)
		}
	}

	return terr
}

func mkdir(ctx *cmd.CliContext, path types.Path, f bool) error {

	opts := make([]types.MkdirOption, 0)

	if f {
		opts = append(opts, client.WithForce())
	}

	return ctx.Client.Mkdir(path, opts...)
}
