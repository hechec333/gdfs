package fs

import (
	"fmt"
	"gdfs/internal/common"
	"gdfs/toolkit/cli/cmd"
	"gdfs/types"
	"io"
	"path"
	"sort"
	"text/tabwriter"
)

type lsOption func(*lsCfg)

type lsCfg struct {
	w io.WriteCloser
}

type SortBy []ModInfo

func (a SortBy) Len() int           { return len(a) }
func (a SortBy) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a SortBy) Less(i, j int) bool { return a[i].Mod < a[j].Mod }

type ModInfo struct {
	Mod  string
	Name string
	Path string
}

type TabWriter struct {
	w *tabwriter.Writer
}

func New(w io.Writer, minwidth, tabwidth, padding int, padchar byte, flag uint) TabWriter {
	return TabWriter{
		w: tabwriter.NewWriter(w, minwidth, tabwidth, padding, padchar, flag),
	}
}

func (t TabWriter) Write(b []byte) (int, error) {
	return t.w.Write(b)
}

func (t TabWriter) Close() error {
	return t.w.Flush()
}

func defaultlsOption() lsCfg {
	return lsCfg{}
}

func WithTabWriter(w io.Writer) lsOption {
	return func(cfg *lsCfg) {
		cfg.w = New(w, 0, 0, 2, ' ', 0)
	}
}

func CallLs(ctx *cmd.CliContext, cmd *cmd.Command) {
	xpath := cmd.Arg[0]

	if xpath[0] != '/' {
		xpath = path.Join(string(ctx.Pwd), xpath)
	}

	err := ls(ctx, types.Path(xpath), WithTabWriter(ctx.StdOut))

	if err != nil {
		ctx.Errorf("ls command error %v", err)
	}
}

func ls(ctx *cmd.CliContext, path types.Path, opts ...lsOption) error {

	cfg := defaultlsOption()

	for _, v := range opts {
		v(&cfg)
	}

	mods, err := ListFile(ctx, path)

	if err != nil {
		return err
	}
	first := "Mod\tName\t"
	cfg.w.Write([]byte(first))
	outs := "%v\t%v\t"
	sort.Sort(SortBy(mods))
	for _, i := range mods {
		cfg.w.Write([]byte(fmt.Sprintf(outs, i.Mod, i.Name)))
	}

	return nil
}

func ListFile(ctx *cmd.CliContext, path types.Path) ([]ModInfo, error) {

	paths, err := ctx.Client.List(path)

	if err != nil {
		return nil, err
	}

	ans := make([]ModInfo, len(paths))

	for idx, v := range paths {
		ans[idx] = ModInfo{
			Name: common.GetFileNameWithExt(v.Path),
			Path: string(v.Path),
		}

		if v.IsDir {
			ans[idx].Mod = "d+"
		} else {
			ans[idx].Mod = "f+"
		}
	}

	return ans, nil
}
