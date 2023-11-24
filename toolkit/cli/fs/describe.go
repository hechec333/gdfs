package fs

import (
	"context"
	"gdfs/internal/types"
	"gdfs/toolkit/cli"
	"io"
)

// gdfsctl descirbe ${file}

func describe(cli *cli.CliContext, path types.Path, w io.Writer) error {

	return nil
}

func getDetail(cli *cli.CliContext, path types.Path) (types.GetFileDetailReply, error) {

	ctx := context.Background()
	arg := types.GetFileDetailArg{
		Path: path,
	}
	reply := types.GetFileDetailReply{}
	err := cli.Client.Do(ctx, "Master.RPCGetFileDetail", &arg, &reply)

	return reply, err
}
