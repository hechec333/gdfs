package fs

import (
	"gdfs/internal/client"
	"gdfs/toolkit/cli/cmd"
	"gdfs/types"
)

func CallTouch(ctx *cmd.CliContext, cmd *cmd.Command) error {

	path := cmd.Arg[0]

	perm := cmd.Flags["mode"]

	_, err := touch(ctx, types.Path(path), transfermode(perm))

	return err
}

func transfermode(perm string) uint8 {

	switch perm {
	case "ro":
		fallthrough
	case "r":
		return types.PermReadOnly
	case "rw":
		return types.PermReadWrite
	case "x":
		return types.PermExcute
	}

	return 255
}

func touch(ctx *cmd.CliContext, path types.Path, perm uint8) (*client.File, error) {
	return ctx.Client.Create(path, client.FileMode(perm))
}
