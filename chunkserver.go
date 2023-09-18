package gdfs

import (
	"gdfs/config"
	chunkserver "gdfs/internal/chunkServer"
	"gdfs/internal/types"
	"os"
	"strconv"
)

func NewChunkServer(uuid int64) *chunkserver.ChunkServer {
	cfg := types.ChunkServerServeConfig{
		RootDir: ".",
	}
	cc := config.GetClusterConfig()
	for _, v := range cc.Cluster.Cs.Nodes {
		if v.Uuid == uuid {
			cfg.Address = types.Addr(v.Address + ":" + v.Port)
		}
	}
	if cfg.Address == "" {
		id, ok := os.LookupEnv("GDFS_UUID")
		if !ok {
			panic("uuid is invalid,check the config file")
		}
		xid, err := strconv.ParseInt(id, 10, 64)
		if err != nil {
			panic("GDFS_UUID is invalid")
		}

		for _, v := range cc.Cluster.Cs.Nodes {
			if v.Uuid == xid {
				cfg.Address = types.Addr(v.Address + ":" + v.Port)
			}
		}
	}
	for _, v := range cc.Cluster.Master.Nodes {
		cfg.MetaServers = append(cfg.MetaServers, types.Addr(v.Address+":"+v.Port))
	}

	return chunkserver.MustNewAndServe(&cfg)
}
