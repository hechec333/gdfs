package master

import (
	"gdfs/types"
	"path"
	"testing"
)

func TestNnDfs(t *testing.T) {

	nsc := NewNamespaceControlor()
	dics := []string{"/opt", "/opt/test", "/var", "/log"}
	files := []string{"/opt/start.sh", "/var/run.pid", "/log/tomcat.log"}
	for _, v := range dics {
		_, f := path.Split(v)
		nsc.applyCreated(types.Path(v), []types.PersiteTreeNode{
			{
				Name:  f,
				IsDir: true,
			},
		})
	}
	for _, v := range files {
		_, f := path.Split(v)
		nsc.applyCreatef(types.Path(v), []types.PersiteTreeNode{
			{
				Name:  f,
				IsDir: true,
			},
		})
	}
	nsc.root.bfs("", func(path types.Path, n *NameSpaceTreeNode) {
		t.Log(path)
	})
}

func TestMaster(t *testing.T) {

}
