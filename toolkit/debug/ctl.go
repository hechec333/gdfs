package main

import (
	"flag"
	"fmt"
	"gdfs/config"
	"gdfs/internal/common/rpc"
	"gdfs/internal/types"
	"log"
	"os"
	"reflect"
	"sync"
	"text/tabwriter"
	"time"
)

func main() {
	var (
		cc    = config.GetClusterConfig()
		check string
	)
	flag.StringVar(&check, "check", "", "-m show master state;-c show chunkserver state")

	flag.Parse()

	switch check {
	case "m":
		PrintMasterRole(cc.Cluster.Master.Nodes)
	case "c":
		PrintChunkServerMaster(cc.Cluster.Cs.Nodes)
	default:
	}
}
func PrintMasterRole(m []config.Node) {
	gm := MasterIdentity(m)
	fmt.Println("**************************")
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	sl := make([]types.Addr, 0)
	for k := range gm {
		fmt.Fprintf(w, "%v\t", k)
		fmt.Fprintf(w, "\t")
		sl = append(sl, k)
	}

	fmt.Fprintln(w)
	for _, v := range sl {
		fmt.Fprintf(w, "%v\t", gm[v])
		fmt.Fprintf(w, "\t")
	}
	fmt.Fprintln(w)
	w.Flush()
}

func PrintChunkServerMaster(cs []config.Node) {
	gm := ChunkServerCurrentMaster(cs)
	fmt.Println("**************************")
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintf(w, "ChunkServer\t")
	val := reflect.ValueOf(types.ReportCurrentMasterAddrReply{})

	for i := 0; i < val.NumField(); i++ {
		fmt.Fprintf(w, "%v\t", val.Type().Field(i).Name)
	}
	fmt.Fprintln(w)
	for k, v := range gm {
		fmt.Fprintf(w, "%v\t", k)
		val := reflect.ValueOf(*v)

		for i := 0; i < val.NumField(); i++ {
			fmt.Fprintf(w, "%v\t", val.Field(i).Interface())
		}
		fmt.Fprintln(w)
	}
	w.Flush()
}

// -check m
func MasterIdentity(m []config.Node) map[types.Addr]string {

	var (
		gm = make(map[types.Addr]string)
		wg = sync.WaitGroup{}
		mu sync.Mutex
	)

	for _, v := range m {
		wg.Add(1)
		go func(peer string) {
			defer wg.Done()
			arg := types.MasterCheckArg{
				Server: ":3611",
			}
			argv := types.MasterCheckReply{}
			err := rpc.Call(types.Addr(peer), "Master.RPCCheckMaster", &arg, &argv, rpc.CallWithTimeOut(3*time.Second))
			mu.Lock()
			defer mu.Unlock()
			if err == rpc.ErrTimeOut {
				gm[types.Addr(peer)] = "Partition"
				return
			}
			if err != nil {
				gm[types.Addr(peer)] = "Unknown Error"
				return
			}

			if argv.Master {
				gm[types.Addr(peer)] = "Master"
			} else {
				gm[types.Addr(peer)] = "Follower"
			}
		}(v.Address + ":" + v.Port)
	}

	wg.Wait()
	return gm
}

// -check c
func ChunkServerCurrentMaster(cs []config.Node) map[types.Addr]*types.ReportCurrentMasterAddrReply {
	var (
		gm = make(map[types.Addr]*types.ReportCurrentMasterAddrReply)
		wg = sync.WaitGroup{}
		mu sync.Mutex
	)

	for _, v := range cs {
		wg.Add(1)
		go func(peer string) {
			defer wg.Done()
			arg := types.ReportCurrentMasterAddrArg{}
			argv := types.ReportCurrentMasterAddrReply{}
			err := rpc.Call(types.Addr(peer), "ChunkServer.RPCReportCurrentMasterAddr", &arg, &argv, rpc.CallWithTimeOut(3*time.Second))
			mu.Lock()
			defer mu.Unlock()
			if err == rpc.ErrTimeOut {
				gm[types.Addr(peer)] = nil
				return
			}
			if err != nil {
				log.Printf("call %v error %v", peer, err)
				return
			}

			gm[types.Addr(peer)] = &argv
		}(v.Address + ":" + v.Port)
	}

	wg.Wait()

	return gm
}
