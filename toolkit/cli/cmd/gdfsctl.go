package cmd

import (
	"fmt"
	"gdfs/internal/client"
	"gdfs/types"
	"io"
	"time"

	"github.com/urfave/cli/v2"
)

type EndPointCfg struct {
	Endpoint []types.Addr
}

type Command struct {
	Cmd   string
	Arg   []string
	Flags map[string]string
}

type CliContext struct {
	EndPointCfg
	Client  client.Client
	History []Command
	Pwd     types.Path
	StdOut  io.Writer
	Env     []string
}

func NewCliContext(cfg EndPointCfg) *CliContext {
	return &CliContext{
		EndPointCfg: cfg,
		Client: *client.NewClient(&client.ClientConfig{
			Master: cfg.Endpoint,
		}),
		History: make([]Command, 0),
		Pwd:     "/",
		Env:     make([]string, 0),
	}
}

func (ctx *CliContext) Errorf(format string, args ...interface{}) {
	args = append(args, time.Now())
	fmt.Fprintf(ctx.StdOut, "%v "+format, args...)
}

func AppendError(err error, err2 error) error {
	if err == nil {
		return err2
	}
	return fmt.Errorf("%s;%s", err, err2)
}

var MasterFlag = &cli.StringSliceFlag{
	Name:    "master",
	Usage:   "指定master结点的地址",
	Aliases: []string{"m"},
	EnvVars: []string{"GDFS_MASTERS"},
	Value:   cli.NewStringSlice("127.0.0.1:8803"),
}

func GetMasters(ctx *cli.Context) []types.Addr {
	s := ctx.Value("master").(cli.StringSlice)
	sl := s.Value()
	m := []types.Addr{}

	for _, endpoint := range sl {
		m = append(m, types.Addr(endpoint))
	}
	return m
}
