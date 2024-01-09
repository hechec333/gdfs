package fs

import (
	"gdfs/internal/client"
	"gdfs/toolkit/cli/cmd"
	"gdfs/types"
	"io"

	"github.com/urfave/cli/v2"
)

// gdfsctl upload ${src} ${dst}
// gdfsctl upload ${dst} 从标准输入读取
func upload(ctx *cli.Context) error {
	return nil
}

// 向gdfs写
func write(ctx *cmd.CliContext, path types.Path, r io.Reader, f bool) (int64, error) {

	flag := client.O_RWONLY
	if f {
		flag |= client.O_CREATE
	}
	file, err := ctx.Client.OpenFile(path, flag)
	if err != nil {
		return -1, err
	}
	return io.Copy(file, r)
}

// gdfsctl download ${dst} ${src}
// gdfsctl download ${dst} 输出到标准输出
func download(ctx *cli.Context) error {
	return nil
}

// 从gdfs读
func read(ctx *cmd.CliContext, path types.Path, w io.Writer) (int64, error) {
	flag := client.O_RWONLY

	file, err := ctx.Client.OpenFile(path, flag)

	if err != nil {
		return -1, err
	}
	return io.Copy(w, file)
}
