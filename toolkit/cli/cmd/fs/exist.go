package fs

import (
	"context"
	"gdfs/toolkit/cli/cmd"
	"gdfs/types"
)

// gdfsctl test -d ${path}
// gdfsctl test -f ${file}
// gdfsctl test -e ${file} 是否空
func Test(ctx *cmd.CliContext, path types.Path, param string) (bool, error) {

	if param == "f" {
		f, ok := isExist(ctx, path)

		if !ok {
			return false, types.ErrPathNotFound
		}

		return !f, nil
	} else if param == "d" {

		f, ok := isExist(ctx, path)

		if !ok {
			return false, types.ErrPathNotFound
		}

		return !f, nil
	} else if param == "e" {

		return isEmpty(ctx, path)
	}

	return false, types.ErrUnReachAble
}

// 判断文件/目录是否存在
func isExist(ctx *cmd.CliContext, path types.Path) (isdir bool, f bool) {
	arg := types.PathExistArg{
		Path: path,
	}

	reply := types.PathExistReply{}

	err := ctx.Client.Do(context.TODO(), "Master.RPCPathTest", arg, &reply)

	return reply.Ok, types.ErrEqual(err, types.ErrPathNotFound)
}

// 判断给定文件是否是空
func isEmpty(ctx *cmd.CliContext, path types.Path) (bool, error) {

	arg := types.GetFileInfoArg{
		Path: path,
	}

	reply := types.GetFileInfoReply{}

	err := ctx.Client.Do(context.TODO(), "Master.RPCGetFileInfo", &arg, &reply)

	return reply.Length == 0, err
}
