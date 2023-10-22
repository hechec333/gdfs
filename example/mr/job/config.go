package mr

import "sync"

type MrConfig struct {
	addr string 
}

var conf *MrConfig
var once sync.Once

func newConfig() {
	conf = &MrConfig{
		addr: "6060",
	}
}

func GetConfig() *MrConfig {
	if conf == nil {
		once.Do(newConfig)
	}

	return conf
}
