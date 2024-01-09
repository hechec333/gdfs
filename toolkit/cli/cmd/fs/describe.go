package fs

import (
	"context"
	"gdfs/toolkit/cli/cmd"
	"gdfs/types"
	"io"
)

// gdfsctl descirbe ${file}

func Describe(cli *cmd.CliContext, path types.Path, w io.Writer) error {

	return nil
}

func getDetail(cli *cmd.CliContext, path types.Path) (types.GetFileDetailReply, error) {

	ctx := context.Background()
	arg := types.GetFileDetailArg{
		Path: path,
	}
	reply := types.GetFileDetailReply{}
	err := cli.Client.Do(ctx, "Master.RPCGetFileDetail", &arg, &reply)

	return reply, err
}
