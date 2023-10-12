package mr

import "gdfs/internal/client"

type Loader struct {
}

func (l *Loader) load() {
	c := client.NewClient(nil)

	c.Create("/sss")

	
}
