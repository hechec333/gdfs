package config

import (
	"encoding/xml"
	"log"
	"os"
	"sync"
)

type ChunkServerConfig struct {
	Nodes []Node `xml:"node"`
}
type MasterConfig struct {
	Nodes []Node `xml:"node"`
}
type Node struct {
	Uuid    int64  `xml:"uuid"`
	Name    string `xml:"name"`
	Address string `xml:"address"`
	Port    string `xml:"port"`
	Debug   string `xml:"debug"`
	Quromn  string `xml:"quromn"`
}
type ClusterConfig struct {
	Master MasterConfig      `xml:"master"`
	Cs     ChunkServerConfig `xml:"chunkserver"`
}

type Configuartion struct {
	Version string        `xml:"version"`
	Cluster ClusterConfig `xml:"clusters"`
}

func SetPath(s string) {
	path = s + "/" + path
}

var path = "config.xml"

func newConfiguration() *Configuartion {
	cc := Configuartion{}
	log.Println("load cluster config file")
	f, err := os.Open(path)
	if err != nil {
		panic(err)
	}

	d := xml.NewDecoder(f)

	// xc := struct {
	// 	Conf Configuartion `xml:"configuration"`
	// }{}

	err = d.Decode(&cc)
	if err != nil {
		panic(err)
	}
	//cc = xc.Conf

	return &cc
}

var conf *Configuartion

func GetClusterConfig() *Configuartion {
	if conf == nil {
		var mu sync.Mutex
		mu.Lock()
		defer mu.Unlock()
		if conf == nil {
			conf = newConfiguration()
		}
	}

	return conf
}
