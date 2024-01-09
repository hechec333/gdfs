package main

import (
	"bufio"
	"errors"
	"fmt"
	"gdfs/toolkit/cli/cmd/fs"
	"gdfs/types"
	"log"
	"os"
	"runtime"
	"strings"

	"github.com/fatih/color"
	"github.com/urfave/cli/v2"
)

var (
	ErrQuit           = errors.New("quit")
	ErrMasterNotFound = errors.New("lack of master address")
)

var env *CliEnv = &CliEnv{}

type CliEnv struct {
	Master []types.Addr // 主节点地址
	Cwd    string       // 当前目录
}

func InitSetup() *cli.App {
	app := cli.App{
		Flags: []cli.Flag{
			&cli.StringSliceFlag{
				Name: "master",
				Value: cli.NewStringSlice(
					"127.0.0.1:2330",
				),
				Aliases: []string{"m"},
				Action: func(ctx *cli.Context, s []string) error {
					if env.Master == nil {
						env.Master = make([]types.Addr, 0)
					}
					for _, v := range s {
						env.Master = append(env.Master, types.Addr(v))
					}
					return nil
				},
			},
		},
		Name: "shell",
		Action: func(ctx *cli.Context) error {
			err := preCheck(ctx)
			if err != nil {
				return err
			}
			fmt.Println("Wecome To Golang Distributed File System!")
			fmt.Printf("Init enviromnet [masters:%v] [cwd:%v]\n", env.Master, env.Cwd)
			return nil
		},
	}
	return &app
}

func InitApp() *cli.App {
	app := cli.App{
		Commands: []*cli.Command{
			{
				Name:        "dfs",
				Aliases:     []string{"fs"},
				Subcommands: fs.Export(),
			},
			{
				Name:    "exit",
				Aliases: []string{"quit"},
				Action: func(ctx *cli.Context) error {
					return ErrQuit
				},
			},
		},
	}
	return &app
}

func enterCommand(r *bufio.Reader) ([]string, error) {
	// [gdfs@/]> dfs touch 1.txt
	fmt.Printf("%s[@%s]%s ", color.BlueString("gdfs://"), color.RedString(env.Cwd), color.GreenString(">"))

	line, err := r.ReadString('\n')
	sep := "\n"
	if runtime.GOOS == "windows" {
		sep = "\r\n"
	}
	line = strings.Trim(line, sep)
	if err != nil {
		return nil, err
	}

	seps := strings.Split(line, " ")

	var strs []string
	strs = append(strs, "gdfs")
	for _, v := range seps {
		if len(v) == 0 {
			continue
		}
		strs = append(strs, v)
	}

	return strs, nil
}

// 检查启动参数
func preCheck(ctx *cli.Context) error {

	if len(env.Master) == 0 {
		return ErrMasterNotFound
	}
	if env.Cwd == "" {
		env.Cwd = "/"
	}
	return nil
}

func loop() {
	defer func() {
		if err := recover(); err != nil {
			log.Println(err)
		}
	}()
	app := InitApp()
	r := bufio.NewReader(os.Stdin)
	for {
		tokens, err := enterCommand(r)
		if err != nil {
			break
		}
		err = app.Run(tokens)
		fmt.Println()
		if err != nil {
			if err == ErrQuit {
				break
			}
			log.Println(err)
		}
	}
}

func main() {
	setup := InitSetup()
	err := setup.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
	loop()
}
