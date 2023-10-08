package gdfs

import (
	"gdfs/config"
	"gdfs/internal/common"
	"gdfs/internal/master"
	"gdfs/internal/types"
	"os"
	"strconv"
)

func NewMaster(uuid int64) *master.Master {

	var (
		cfg = types.MetaServerServeConfig{}

		cc = config.GetClusterConfig()
		me = -1
	)

	for _, v := range cc.Cluster.Master.Nodes {
		if v.Uuid == uuid {
			me = int(v.Uuid) % len(cc.Cluster.Master.Nodes)
		}
	}

	if me == -1 {
		id, ok := os.LookupEnv("GDFS_UUID")
		if !ok {
			panic("uuid is invalid,check the config file")
		}
		xid, err := strconv.ParseInt(id, 10, 64)
		if err != nil {
			panic("GDFS_UUID is invalid")
		}

		me = int(xid) % len(cc.Cluster.Master.Nodes)
	}

	for _, v := range cc.Cluster.Master.Nodes {
		cfg.Servers = append(cfg.Servers, types.Addr(v.Address+":"+v.Port))
		cfg.Protocol = append(cfg.Protocol, types.Addr(v.Address+":"+v.Quromn))
	}
	cfg.Me = me
	common.MusOpenLogFile(&cc.Cluster.Master.Nodes[me])
	return master.MustNewAndServe(&cfg)
}
