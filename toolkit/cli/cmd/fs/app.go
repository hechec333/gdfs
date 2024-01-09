package fs

import (
	"errors"
	"gdfs/toolkit/cli/cmd"

	"github.com/urfave/cli/v2"
)

var (
	ErrNotEnoughArgs = errors.New("not enough args")
	ErrInvalidArg    = errors.New("invalid argument")
)
var command []*cli.Command

func register(xcmd *cli.Command) {
	if command == nil {
		command = make([]*cli.Command, 0)
	}
	xcmd.Flags = append(xcmd.Flags, cmd.MasterFlag)
	command = append(command, xcmd)
}

func Export() []*cli.Command {
	return command
}
