package internal_test

import (
	chunkserver "gdfs/internal/chunkServer"
	"gdfs/internal/client"
	"gdfs/internal/master"
	"gdfs/internal/types"
	"testing"
	"time"
)

func TestM(t *testing.T) {

}

func TestMaster(t *testing.T) {

	masters := []types.Addr{"127.0.0.1:3880", "127.0.0.1:3889"}
	protocols := []types.Addr{"127.0.0.1:4881", "127.0.0.1:4882"}
	chunks := []types.Addr{"127.0.0.1:4667", "127.0.0.1:4668", "127.0.0.1:4669"}

	for i := range masters {
		master.MustNewAndServe(&types.MetaServerServeConfig{
			Me:       i,
			Servers:  masters,
			Protocol: protocols,
		})
	}

	for _, v := range chunks {
		chunkserver.MustNewAndServe(&types.ChunkServerServeConfig{
			Address:     v,
			MetaServers: masters,
			RootDir:     ".",
		})
	}

	time.Sleep(time.Second)
	cli := client.NewClient(masters)
	var err error
	err = cli.Mkdir("/opt/log/lazy", client.WithForce())
	if err != nil {
		t.Fatal(err)
	}
	err = cli.Create("/opt/log/lazy/repot.txt")
	if err != nil {
		t.Log(err)
	}

	err = cli.Create("/opt/log/gin.log")
	if err != nil {
		t.Log(err)
	}

	err = cli.Mkdir("/opt/zip/run", client.WithForce())
	if err != nil {
		t.Log(err)
	}
	time.Sleep(2 * time.Second)

	cli.Walk("/opt", func(p types.Path) {
		t.Log(p)
	})
}
