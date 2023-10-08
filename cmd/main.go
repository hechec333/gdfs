package main

import (
	"flag"
	"gdfs"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {

	var r string
	var id int64
	flag.StringVar(&r, "r", "", "role -m <master> -c <chunkserver>")
	flag.Int64Var(&id, "u", 0, "id ")
	flag.Parse()
	log.Println("Server Start With Role ", r)
	// config.SetPath("..")
	switch r {
	case "m":
		m := gdfs.NewMaster(id)
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt, syscall.SIGTERM)
		<-c
		m.Stop()
		log.Println("Recevice exit signal")
	case "c":
		cs := gdfs.NewChunkServer(id)
		c := make(chan os.Signal, 1)

		signal.Notify(c, os.Interrupt, syscall.SIGTERM)

		<-c
		cs.Stop()
		log.Println("Recevice exit signal")
	}
}
