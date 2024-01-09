package internal_test

import (
	"fmt"
	chunkserver "gdfs/internal/chunkServer"
	"gdfs/internal/client"
	"gdfs/internal/master"
	"gdfs/types"
	"io"
	"strings"
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
func TestRecover(t *testing.T) {
	masters := []types.Addr{"127.0.0.1:3880"}
	protocols := []types.Addr{"127.0.0.1:4881"}
	chunks := []types.Addr{"127.0.0.1:4667", "127.0.0.1:4668", "127.0.0.1:4669"}

	initCluseter(masters, protocols, chunks)

	time.Sleep(3 * time.Second)
	cli := client.NewClient(&client.ClientConfig{
		Master: masters,
	})

	// err := cli.Mkdir("/opt/test", client.WithForce())
	// if err != nil {
	// 	t.Log(err)
	// }
	err := cli.Walk("/", func(p types.Path) {
		t.Log(p)
	})

	t.Log(err)
}

func TestCreateFile(t *testing.T) {

	masters := []types.Addr{"127.0.0.1:3880", "127.0.0.1:3881"}
	protocols := []types.Addr{"127.0.0.1:4881", "127.0.0.1:4882"}
	chunks := []types.Addr{"127.0.0.1:4667", "127.0.0.1:4668", "127.0.0.1:4669"}

	initCluseter(masters, protocols, chunks)

	time.Sleep(time.Second)
	cli := client.NewClient(&client.ClientConfig{
		Master: masters,
	})
	var err error
	err = cli.Mkdir("/opt/log", client.WithForce())
	if err != nil {
		t.Fatal(err)
	}
	err = cli.Mkdir("/opt/log/lazy")
	if err != nil {
		t.Fatal(err)
	}
	_, err = cli.Create("/opt/log/lazy/repot.txt", client.GDFS_RWONLY)
	if err != nil {
		t.Log(err)
	}

	_, err = cli.Create("/opt/log/gin.log", client.GDFS_RWONLY)
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

	addrs, err := cli.List("/opt")
	t.Log(err)
	t.Log(addrs)
}

func TestFileRW(t *testing.T) {
	masters := []types.Addr{"127.0.0.1:3880"}
	protocols := []types.Addr{"127.0.0.1:4881"}
	chunks := []types.Addr{"127.0.0.1:4667", "127.0.0.1:4668", "127.0.0.1:4669"}

	initCluseter(masters, protocols, chunks)

	time.Sleep(3 * time.Second)

	cli := client.NewClient(&client.ClientConfig{
		Master: masters,
	})

	err := cli.Mkdir("/opt/test", client.WithForce())

	if err != nil {
		t.Fatal(err)
	}

	f, err := cli.Create("/opt/test/test.txt", client.GDFS_RWONLY)
	if err != nil {
		t.Fatal(err)
	}

	var text string = "aabbcc"

	w, err := io.Copy(f, strings.NewReader(text))

	if err != nil {
		t.Fatalf("%v", err)
	}

	t.Logf("written %v bytes", w)

	//str := bytes.Buffer{}
	bs, err := io.ReadAll(f)
	if err != nil {
		t.Fatalf("%v", err)
	}

	t.Logf("readden %v bytes", len(bs))

	fmt.Println(string(bs))
}
