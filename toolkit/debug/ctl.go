package main

import (
	"flag"
	"fmt"
	"gdfs/config"
	"gdfs/internal/types"
	"os"
	"reflect"
	"text/tabwriter"
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
