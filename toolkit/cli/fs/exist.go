package fs

import (
	"context"
	"gdfs/internal/types"
	"gdfs/toolkit/cli"
)

// gdfsctl test -d ${path}
// gdfsctl test -f ${file}
func test(ctx *cli.CliContext, path types.Path, param string) (bool, error) {

}

// 判断文件/目录是否存在
func isExist(ctx *cli.CliContext, path types.Path) (isdir bool, f bool) {
	arg := types.PathExistArg{
		Path: path,
	}

	reply := types.PathExistReply{}

	err := ctx.Client.Do(context.TODO(), "Master.RPCPathTest", arg, &reply)

	return reply.Ok, types.ErrEqual(err, types.ErrPathNotFound)
}

// 判断给定文件是否是空
func isEmpty(ctx *cli.CliContext, path types.Path) (bool, error) {

	arg := types.GetFileInfoArg{
		Path: path,
	}

	reply := types.GetFileInfoReply{}

	err := ctx.Client.Do(context.TODO(), "Master.RPCGetFileInfo", &arg, &reply)

	return reply.Length == 0, err
}
