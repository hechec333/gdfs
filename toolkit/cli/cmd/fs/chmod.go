package fs

import (
	"context"
	"gdfs/toolkit/cli/cmd"
	"gdfs/types"
	"os"
	"text/tabwriter"

	"github.com/urfave/cli/v2"
)

func init() {
	register(&cli.Command{
		Name: "chmod",
		Action: func(ctx *cli.Context) error {
			paths, perm, err := chmod(ctx)
			if err != nil {
				return err
			}
			w := tabwriter.NewWriter(os.Stdout, 0, 4, 0, 1, 0)
			w.Write([]byte("Files\tMode\n"))
			for _, path := range paths {
				w.Write([]byte(path + "\t" + types.PermMap[perm] + "\n"))
			}
			w.Flush()
			return nil
		},
	})

	register(&cli.Command{
		Name: "perm",
		Action: func(ctx *cli.Context) error {
			paths, datas, err := Perm(ctx)
			if err != nil {
				return err
			}

			w := tabwriter.NewWriter(os.Stdout, 0, 4, 0, 1, 0)
			w.Write([]byte("File\tMode\n"))

			for idx, data := range datas {
				w.Write([]byte(string(paths[idx]) + "\t" + types.PermMap[data] + "\n"))
			}
			w.Flush()
			return nil
		},
	})
}

func chmod(ctx *cli.Context) ([]string, types.FilePerm, error) {
	num := ctx.NArg()
	if num < 2 {
		return nil, 0, ErrNotEnoughArgs
	}
	args := ctx.Args()
	var perm types.FilePerm
	switch args.Get(num - 1) {
	case "+ro", "ro", "775":
		perm = types.PermReadOnly
	case "+rw", "rw", "777":
		perm = types.PermReadWrite
	case "+x", "x", "773":
		perm = types.PermExcute
	default:
		return nil, 0, ErrInvalidArg
	}
	var terr error
	var ans []string
	cc := cmd.NewCliContext(cmd.EndPointCfg{
		Endpoint: cmd.GetMasters(ctx),
	})
	for idx := 0; idx < num-1; idx++ {
		path := ctx.Args().Get(idx)
		ans = append(ans, path)
		err := Chmod(cc, types.Path(path), perm)
		if err != nil {
			terr = cmd.AppendError(terr, err)
		}
	}
	return ans, perm, terr
}

// gdfsctl dfs chmod $file... +ro +rw
func Chmod(ctx *cmd.CliContext, path types.Path, perm types.FilePerm) error {

	arg := types.SetFilePermArg{
		Path: path,
		Perm: perm,
	}

	reply := types.SetFilePermReply{}

	err := ctx.Client.Do(context.TODO(), "Master.RPCSetFilePerm", arg, &reply)

	return err
}

func Perm(ctx *cli.Context) ([]string, []types.FilePerm, error) {
	if ctx.NArg() < 1 {
		return nil, nil, ErrNotEnoughArgs
	}
	var (
		paths []string
		perms []types.FilePerm
		terr  error
		cli   = cmd.NewCliContext(cmd.EndPointCfg{
			Endpoint: cmd.GetMasters(ctx),
		})
	)
	num := ctx.NArg()
	for idx := 0; idx < num; idx++ {
		perm, err := GetPerm(cli, types.Path(ctx.Args().Get(idx)))
		if err != nil {
			terr = cmd.AppendError(terr, err)
		}
		perms = append(perms, perm)
		paths = append(paths, ctx.Args().Get(idx))
	}

	return paths, perms, terr
}

// gdfsctl dfs perm
func GetPerm(ctx *cmd.CliContext, path types.Path) (perm types.FilePerm, err error) {
	arg := types.GetFilePermArg{
		Path: path,
	}

	reply := types.GetFilePermReply{}
	err = ctx.Client.Do(context.TODO(), "Master.RPCGetFilePerm", arg, &reply)
	perm = reply.Info.Mode.Perm
	return
}
