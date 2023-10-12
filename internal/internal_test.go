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

func initCluseter(m []types.Addr, np []types.Addr, cks []types.Addr) {
	for i := range m {
		master.MustNewAndServe(&types.MetaServerServeConfig{
			Me:       i,
			Servers:  m,
			Protocol: np,
		})
	}

	for _, v := range cks {
		chunkserver.MustNewAndServe(&types.ChunkServerServeConfig{
			Address:     v,
			MetaServers: m,
			RootDir:     ".",
		})
	}
}

func TestCreateFile(t *testing.T) {

	masters := []types.Addr{"127.0.0.1:3880", "127.0.0.1:3889"}
	protocols := []types.Addr{"127.0.0.1:4881", "127.0.0.1:4882"}
	chunks := []types.Addr{"127.0.0.1:4667", "127.0.0.1:4668", "127.0.0.1:4669"}

	initCluseter(masters, protocols, chunks)

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

func TestFileRW(t *testing.T) {
	masters := []types.Addr{"127.0.0.1:3880", "127.0.0.1:3889"}
	protocols := []types.Addr{"127.0.0.1:4881", "127.0.0.1:4882"}
	chunks := []types.Addr{"127.0.0.1:4667", "127.0.0.1:4668", "127.0.0.1:4669"}

	initCluseter(masters, protocols, chunks)

	time.Sleep(time.Second)

	cli := client.NewClient(masters)

	err := cli.Mkdir("/opt/test", client.WithForce())

	if err != nil {
		t.Log(err)
	}
}
