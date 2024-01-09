package master

import (
	"context"
	"fmt"
	"gdfs/internal/common"
	"gdfs/types"
	"gdfs/internal/wal"
	"log"
	"path"
	"strings"
	"sync"
	"time"
)

type FileMode struct {
	perm         types.FilePerm
	lastModefied time.Time
	createTime   time.Time
}

type NameSpaceControlor struct {
	root *NameSpaceTreeNode
}
type NameSpaceTreeNode struct {
	sync.RWMutex
	name  string
	isDir bool
	// dir property
	children []*NameSpaceTreeNode

	// file property
	length int64
	chunks int64

	mode *FileMode
}

func (nst *NameSpaceTreeNode) isExist(name string) bool {
	for _, v := range nst.children {
		if v.name == name {
			return true
		}
	}

	return false
}

func NewNsTree(root types.Path) *NameSpaceTreeNode {
	paths := strings.Split(string(root), "/")[1:]
	ns := &NameSpaceTreeNode{
		isDir: true,
		name:  paths[0],
	}
	if len(paths) > 1 {
		ns.children = append(ns.children, NewNsTree(types.Path(strings.Join(paths[1:], "/"))))
	}

	return ns
}

func NewNamespaceControlor() *NameSpaceControlor {
	return &NameSpaceControlor{
		root: NewNsTree("/"),
	}
}

// 锁上从根路径到给定路径的父目录的读锁，不锁定父目录
// 返回根路径到父目录上的所有目录名称，以及父目录
func (nsc *NameSpaceControlor) lockUpperPath(path types.Path, lock bool) ([]string, *NameSpaceTreeNode, error) {
	parent, _ := common.PartionPath(path)
	root := nsc.root
	if lock {
		root.RLock()
	}
	depth := 1
	if len(parent) > 1 {
		for _, v := range parent[1:] {
			for _, vv := range root.children {
				if v == vv.name {
					if lock {
						root.RLock()
					}
					root = vv
					depth++
					break
				}
			}
		}
	}

	if len(parent) != depth {
		return parent[:depth], root, types.ErrPathNotFound
	}
	// if lock {
	// 	//root.RLock()
	// 	//root.RUnlock() //不锁定父级
	// }
	return parent, root, nil
}

func (nsc *NameSpaceControlor) unlockUpperPath(path types.Path) {
	parent, _ := common.PartionPath(path)

	root := nsc.root
	//root.RUnlock()
	depth := 1
	stack := make([]*NameSpaceTreeNode, len(parent))
	stack[0] = root
	if len(parent) > 1 {
		for _, v := range parent[1:] {
			for _, vv := range root.children {
				if vv.name == v {
					stack[depth] = vv
					depth++
					root = vv
					break
				}
			}
		}
	}
	if depth == 1 {
		root.RUnlock()
	}
	for i := depth - 2; i >= 0; i-- {
		stack[i].RUnlock()
	}
}

func (nsc *NameSpaceControlor) CreateFileImpl(do *wal.LogOpLet, path types.Path, perm types.FilePerm) error {
	_, cwd, err := nsc.lockUpperPath(path, true)
	defer nsc.unlockUpperPath(path)
	if err != nil {
		return err
	}

	cwd.Lock()
	defer cwd.Unlock()

	// cwd.children = append(cwd.children, &NameSpaceTreeNode{
	// 	isDir:  false,
	// 	name:GetFileNameWithExt(path),
	// 	chunks: 0,
	// 	length: 0,
	// })
	err = nsc.MustCreateFile(do, path, common.GetFileNameWithExt(path), perm)
	if err != nil {
		panic(err)
	}
	return nil
}

func extractCommonPath(paths []types.Path) (string, []string, error) {
	if len(paths) < 1 {
		return "", nil, types.ErrInvalidArgument
	}

	prefix, _ := common.PartionPath(paths[0])

	commonprefix := path.Join(prefix...)

	names := make([]string, len(paths))

	for i := 0; i < len(paths); i++ {
		str := strings.TrimPrefix(string(paths[i]), commonprefix)
		// 所有path必须同一个父路径
		if str == string(paths[i]) {
			return "", nil, types.ErrInvalidArgument
		}

		names[i] = str
	}

	return commonprefix, names, nil
}

func (nsc *NameSpaceControlor) CreateBatchFileImpl(do *wal.LogOpLet, perm types.FilePerm, paths ...types.Path) error {

	commonprefix, names, err := extractCommonPath(paths)
	if err != nil {
		return err
	}
	_, _, err = nsc.lockUpperPath(types.Path(commonprefix+"/x"), true)

	if err != nil {
		return err
	}

	defer nsc.unlockUpperPath(types.Path(commonprefix + "/x"))

	err = nsc.MustCreateBatchFile(do, types.Path(commonprefix+"/x"), perm, names)

	if err != nil {
		panic(err)
	}

	return nil
}

func (nsc *NameSpaceControlor) CreateBatchDicImpl(do *wal.LogOpLet, paths ...types.Path) error {
	commonprefix, names, err := extractCommonPath(paths)
	if err != nil {
		return err
	}

	_, _, err = nsc.lockUpperPath(types.Path(commonprefix+"/x"), true)

	if err != nil {
		return err
	}
	defer nsc.unlockUpperPath(types.Path(commonprefix + "/x"))

	err = nsc.MustBatchMkdir(do, types.Path(commonprefix+"/x"), names)

	if err != nil {
		panic(err)
	}

	return nil
}

func (nsc *NameSpaceControlor) DeleteBatchFileImpl(do *wal.LogOpLet, paths ...types.Path) error {
	commonprefix, names, err := extractCommonPath(paths)
	if err != nil {
		return err
	}
	_, _, err = nsc.lockUpperPath(types.Path(commonprefix+"/x"), true)

	if err != nil {
		return err
	}
	defer nsc.unlockUpperPath(types.Path(commonprefix + "/x"))

	err = nsc.MustDeleteBatchFile(do, types.Path(commonprefix+"/x"), names)

	if err != nil {
		panic(err)
	}

	return nil
}

func (nsc *NameSpaceControlor) DeleteFileImpl(do *wal.LogOpLet, xpath types.Path) error {
	_, cwd, err := nsc.lockUpperPath(xpath, true)

	if err != nil {
		return err
	}

	defer nsc.unlockUpperPath(xpath)

	cwd.Lock()
	defer cwd.Unlock()
	filename := common.GetFileNameWithExt(xpath)

	// for _, v := range cwd.children {
	// 	if v.name == filename {
	// 		// mark delete file flag
	// 		v.name = time.Now().Format("2006-01-02_15-04-05") + "@" + path.Base(v.name) + ".del"
	// 		break
	// 	}
	// }
	err = nsc.MustDeleteFile(do, xpath, filename)
	if err != nil {
		panic(err)
	}
	return nil
}

func (nsc *NameSpaceControlor) RecursiveMkdirImpl(do *wal.LogOpLet, path types.Path) error {

	p, cwd, err := nsc.lockUpperPath(path, true)
	if err != nil {
		if err == types.ErrPathNotFound {
			var (
				idx int
			)
			xps := strings.Split(string(path), "/")
			for i, v := range xps {
				if i >= len(p) || v != p[i] {
					break
				}
				idx++
			}
			nsc.unlockUpperPath(path)
			rest := xps[idx:]
			cwd.Lock()
			cr := &NameSpaceTreeNode{
				isDir: true,
				name:  rest[0],
			}
			xr := cr
			for _, v := range rest[1:] {
				cr.children = append(cr.children, &NameSpaceTreeNode{
					name:  v,
					isDir: true,
				})
				cr = cr.children[0]
			}
			defer cwd.Unlock()
			var xpt string = strings.Join(xps[:idx], "/")

			err = nsc.MustMkdir(do, types.Path(xpt), xr)
		}
		return err
	} else {
		return types.ErrUnReachAble
	}
}

func (nsc *NameSpaceControlor) MkdirImpl(do *wal.LogOpLet, path types.Path, r bool) error {
	if r {
		return nsc.RecursiveMkdirImpl(do, path)
	}
	_, cwd, err := nsc.lockUpperPath(path, true)
	if err != nil {
		return err
	}
	defer nsc.unlockUpperPath(path)

	cwd.Lock()
	defer cwd.Unlock()
	filename := common.GetFileNameWithExt(path)

	idx := strings.LastIndex(string(path), "/")
	path = path[:idx]

	err = nsc.MustMkdir(do, path, &NameSpaceTreeNode{
		name:     filename,
		isDir:    true,
		children: []*NameSpaceTreeNode{},
	})
	if err != nil {
		panic(err)
	}
	// cwd.children = append(cwd.children, &NameSpaceTreeNode{
	// 	isDir: true,
	// 	name:  filename,
	// })
	return nil
}

func (nsc *NameSpaceControlor) GetList(path types.Path) ([]types.PathInfo, error) {
	_, cwd, err := nsc.lockUpperPath(path, true)
	list := []types.PathInfo{}
	defer nsc.unlockUpperPath(path)
	if err != nil {
		return list, err
	}

	cwd.RLock()
	defer cwd.RUnlock()
	// todo Distinguish Root problem
	var parent *NameSpaceTreeNode
	if path == "/" {
		return nsc.getRootList(), nil
	} else {
		filename := common.GetFileNameWithExt(path)
		for _, v := range cwd.children {
			if v.name == filename {
				parent = v
				break
			}
		}
	}
	parent.Lock()
	defer parent.Unlock()
	for _, v := range parent.children {
		list = append(list, types.PathInfo{
			Path:   path + "/" + types.Path(v.name),
			IsDir:  v.isDir,
			Chunks: v.chunks,
			Length: v.length,
		})
	}

	return list, nil
}

func (nsc *NameSpaceControlor) getRootList() (res []types.PathInfo) {

	for _, v := range nsc.root.children {
		res = append(res, types.PathInfo{
			Path:   "/" + types.Path(v.name),
			Chunks: v.chunks,
			Length: v.length,
			IsDir:  v.isDir,
		})
	}
	return
}

func (nsc *NameSpaceControlor) GetChildrenInfoImpl(path types.Path) ([]types.FileInfo, error) {
	_, cwd, err := nsc.lockUpperPath(path, true)
	childs := []types.FileInfo{}

	defer nsc.unlockUpperPath(path)
	if err != nil {
		return childs, err
	}
	cwd.RLock()
	defer cwd.RUnlock()
	var parent *NameSpaceTreeNode
	filename := common.GetFileNameWithExt(path)
	for _, v := range cwd.children {
		if v.name == filename {
			parent = v
			break
		}
	}
	parent.RLock()
	defer parent.RUnlock()
	for _, v := range parent.children {
		if !v.isDir && !v.IsMark() {
			childs = append(childs, types.FileInfo{
				Path:   path + "/" + types.Path(v.name),
				Chunks: v.chunks,
				Length: v.length,
				Mode: types.PermInfo{
					Perm:         v.mode.perm,
					LastModefied: v.mode.lastModefied,
					CreateTime:   v.mode.createTime,
				},
			})
		}
	}
	return childs, nil
}

func (nsc *NameSpaceControlor) GetFilePerm(path types.Path) (types.FileInfo, error) {
	_, cwd, err := nsc.lockUpperPath(path, true)
	info := types.FileInfo{}
	defer nsc.unlockUpperPath(path)
	if err != nil {
		return info, err
	}

	name := common.GetFileNameWithExt(path)

	for _, v := range cwd.children {
		if v.IsMark() {
			continue
		}

		if name == v.name && !v.isDir {
			info.Chunks = v.chunks
			info.Path = path
			info.Length = v.length
			info.Mode = types.PermInfo{
				Perm:         v.mode.perm,
				LastModefied: v.mode.lastModefied,
				CreateTime:   v.mode.createTime,
			}

			return info, nil
		}

	}

	return info, types.ErrPathNotFound
}

func (nsc *NameSpaceControlor) GetFileInfoImpl(path types.Path) (types.PathInfo, error) {
	_, cwd, err := nsc.lockUpperPath(path, true)
	info := types.PathInfo{}
	defer nsc.unlockUpperPath(path)
	if err != nil {
		return info, err
	}

	cwd.RLock()
	defer cwd.RUnlock()
	file := common.GetFileNameWithExt(path)
	for _, v := range cwd.children {
		if v.name == file {
			info.IsDir = v.isDir
			info.Chunks = v.chunks
			info.Length = v.length
			info.Path = path

			return info, nil
		}
	}

	return info, fmt.Errorf("file %v not found", path)
}
func (n *NameSpaceTreeNode) bfs(prefix string, f func(path types.Path, n *NameSpaceTreeNode)) {
	n.RLock()
	defer n.RUnlock()
	for i := range n.children {
		t := path.Join(prefix, n.children[i].name)
		f(types.Path(t), n.children[i])
		n.children[i].bfs(t, f)
	}
}
func (nsc *NameSpaceControlor) NodeScan(root types.Path, f func(path types.Path)) ([]types.PersiteTreeNode, error) {
	array := []types.PersiteTreeNode{}

	_, cwd, err := nsc.lockUpperPath(root, false)
	if err != nil {
		return nil, err
	}
	cwd.bfs("", func(path types.Path, n *NameSpaceTreeNode) {
		array = append(array, types.PersiteTreeNode{
			Name:   string(path),
			IsDir:  n.isDir,
			Length: n.length,
			Chunks: n.chunks,
			Mode: types.PermInfo{
				Perm:         n.mode.perm,
				LastModefied: n.mode.lastModefied,
				CreateTime:   n.mode.createTime,
			},
		})
		if f != nil {
			f(path)
		}
	})

	return array, nil
}

func (nsc *NameSpaceControlor) GetDeletedFile() []types.FileInfo {
	files := []types.FileInfo{}
	nsc.root.GetAllMarkedFile("", files)
	return files
}

func (nsc *NameSpaceControlor) PathTest(path types.Path) (bool, error) {
	_, cwd, err := nsc.lockUpperPath(path, true)

	defer nsc.unlockUpperPath(path)

	if err != nil {
		return false, err
	}

	cwd.RLock()
	defer cwd.RUnlock()
	f := common.GetFileNameWithExt(path)

	for _, v := range cwd.children {
		if v.name == f {
			return v.isDir, nil
		}
	}

	return false, types.ErrPathNotFound
}

func (nsc *NameSpaceControlor) PathExist(path types.Path, dir bool) (bool, error) {
	_, cwd, err := nsc.lockUpperPath(path, true)

	defer nsc.unlockUpperPath(path)

	if err != nil {
		return false, err
	}

	cwd.RLock()
	defer cwd.RUnlock()
	f := common.GetFileNameWithExt(path)
	for _, v := range cwd.children {
		if v.isDir == dir && v.name == f {
			return true, nil
		}
	}
	return false, nil
}

func (nsc *NameSpaceControlor) FetchAllDeletedFiles(do *wal.LogOpLet) ([]types.FileInfo, error) {
	files := []types.FileInfo{}
	nsc.root.GetAllMarkedFile("", files)
	errs := []error{}
	for _, v := range files {
		log := types.NsLogImpl{
			CommandType: types.CommandDelete,
			Path:        v.Path,
			File: types.PersiteTreeNode{
				IsDir: false,
				Name:  common.GetFileNameWithExt(v.Path),
			},
		}
		ctx, h := context.WithTimeout(context.TODO(), 1*time.Second)
		defer h()
		err := do.NsStartCtx(ctx, log)
		if err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) != 0 {
		return files, common.JoinErrors(errs...)
	}
	return files, nil
}

func (nst *NameSpaceTreeNode) GetAllMarkedFile(path types.Path, files []types.FileInfo) {
	nst.RLock()
	defer nst.RUnlock()
	if nst.isDir {
		for _, v := range nst.children {
			if v.isDir {
				v.GetAllMarkedFile(path+types.Path("/"+v.name), files)
			} else if v.IsMark() {
				files = append(files, types.FileInfo{
					Path:   path + types.Path("/"+v.name),
					Chunks: v.chunks,
					Length: v.length,
				})
			}
		}
	}
}

func (nsc *NameSpaceControlor) SetFilePerm(do *wal.LogOpLet, path types.Path, perm types.FilePerm) error {
	_, cwd, err := nsc.lockUpperPath(path, true)

	defer nsc.unlockUpperPath(path)

	if err != nil {
		return err
	}

	cwd.RLock()
	defer cwd.RUnlock()
	f := common.GetFileNameWithExt(path)
	//var st *NameSpaceTreeNode
	for _, v := range cwd.children {
		if !v.isDir && !v.IsMark() && v.name == f {
			return v.MustUpdatePerm(do, path, perm)
		}
	}

	return types.ErrPathNotFound
}

// only mark file
func (nst *NameSpaceTreeNode) MarkDeleted() {
	if nst.isDir {
		return
	}
	nst.name = time.Now().Format("2006-01-02_15-04-05") + "#" + path.Base(nst.name) + ".del"
}
func (nst *NameSpaceTreeNode) IsMark() bool {
	return path.Ext(nst.name) == ".del"
}
func (nst *NameSpaceTreeNode) GetChild(name string) (*NameSpaceTreeNode, bool) {
	for _, v := range nst.children {
		if v.name == name {
			return v, true
		}
	}
	return nil, false
}

func (nst *NameSpaceTreeNode) Serial(array *[]types.PersiteTreeNode) int {
	st := types.PersiteTreeNode{
		IsDir:    nst.isDir,
		Name:     nst.name,
		Chunks:   nst.chunks,
		Length:   nst.length,
		Children: make([]int, 0),
	}

	if !st.IsDir {
		st.Mode = types.PermInfo{
			Perm:         nst.mode.perm,
			LastModefied: nst.mode.lastModefied,
			CreateTime:   nst.mode.createTime,
		}
	}

	index := len(*array)
	*array = append(*array, st)
	for _, v := range nst.children {
		st.Children = append(st.Children, v.Serial(array))
	}
	(*array)[index] = st
	return index
}

func (nst *NameSpaceTreeNode) Deserial(array []types.PersiteTreeNode, index int) *NameSpaceTreeNode {
	nst.chunks = array[index].Chunks
	nst.isDir = array[index].IsDir
	nst.length = array[index].Length
	nst.name = array[index].Name
	nst.mode = &FileMode{
		perm:         array[index].Mode.Perm,
		lastModefied: array[index].Mode.LastModefied,
		createTime:   array[index].Mode.CreateTime,
	}
	for _, v := range array[index].Children {
		node := &NameSpaceTreeNode{}
		nst.children = append(nst.children, node.Deserial(array, v))
	}
	return nst
}

func (nsc *NameSpaceControlor) SavePersiteState() []types.PersiteTreeNode {

	nsc.root.RLock()
	defer nsc.root.RUnlock()
	array := []types.PersiteTreeNode{}
	nsc.root.Serial(&array)

	return array
}

func (nsc *NameSpaceControlor) ReadPersiteState(array []types.PersiteTreeNode) error {
	if len(array) == 0 {
		log.Println("empty persitent state")
	}
	nsc.root = nsc.root.Deserial(array, 0)

	return nil
}

// lock by caller
func (nsc *NameSpaceControlor) applyChangeFileInfo(path types.Path, meta types.PersiteTreeNode) error {
	_, cwd, err := nsc.lockUpperPath(path, false)
	if err != nil {
		return err
	}
	for _, v := range cwd.children {
		if v.name == meta.Name {
			v.chunks = meta.Chunks
			v.length = meta.Length
			v.mode = &FileMode{
				perm:         meta.Mode.Perm,
				lastModefied: meta.Mode.LastModefied,
				createTime:   meta.Mode.CreateTime,
			}
		}
	}
	return nil
}

// 递归建目录
func (nsc *NameSpaceControlor) applyCreated(path types.Path, meta []types.PersiteTreeNode) error {
	_, cwd, err := nsc.lockUpperPath(path, false)
	if err != nil {
		return err
	}

	st := NameSpaceTreeNode{}

	st.Deserial(meta, 0)

	cwd.children = append(cwd.children, &st)

	return nil
}

// 批量创建目录
func (nsc *NameSpaceControlor) applyBatchMkdir(path types.Path, meta [][]types.PersiteTreeNode) error {

	_, cwd, err := nsc.lockUpperPath(path, false)

	if err != nil {
		return err
	}

	for _, v := range meta {

		st := NameSpaceTreeNode{}

		st.Deserial(v, 0)

		cwd.children = append(cwd.children, &st)
	}

	return nil
}

// 批量创建文件
func (nsc *NameSpaceControlor) applyCreatef(path types.Path, meta []types.PersiteTreeNode) error {
	_, cwd, err := nsc.lockUpperPath(path, false)
	if err != nil {
		return err
	}
	var children []*NameSpaceTreeNode = nil
	var errs []error
	// if meta.IsDir {
	// 	children = make([]*NameSpaceTreeNode, 0)
	// }
	for _, node := range meta {

		if node.IsDir {
			children = make([]*NameSpaceTreeNode, 0)
		} else {
			children = nil
		}
		if cwd.isExist(node.Name) {
			errs = append(errs, types.Errln(types.ErrPathExists, node.Name))
			continue
		}
		cwd.children = append(cwd.children, &NameSpaceTreeNode{
			name:     node.Name,
			isDir:    node.IsDir,
			children: children,
			chunks:   node.Chunks,
			length:   node.Length,
			mode: &FileMode{
				perm:         node.Mode.Perm,
				lastModefied: node.Mode.LastModefied,
				createTime:   node.Mode.CreateTime,
			},
		})
	}
	return common.JoinErrors(errs...)
}

// 批量删除
func (nsc *NameSpaceControlor) applyDelete(path types.Path, meta []types.PersiteTreeNode) error {
	_, cwd, err := nsc.lockUpperPath(path, false)
	if err != nil {
		return err
	}
	var errs []error
	sl := make(map[string]types.PersiteTreeNode)

	for idx, v := range meta {
		sl[v.Name] = meta[idx]
	}

	for _, v := range cwd.children {
		if _, ok := sl[v.name]; ok {
			v.MarkDeleted()
		} else {
			errs = append(errs, types.Errln(types.ErrNotExist, v.name))
		}
	}
	return common.JoinErrors(errs...)
}
func (nst *NameSpaceTreeNode) MustAddLength(do *wal.LogOpLet, path types.Path, lengths int64) error {
	nst.RLock()
	log := types.PersiteTreeNode{
		IsDir:  false,
		Name:   nst.name,
		Length: nst.length + lengths,
		Chunks: nst.chunks,
		Mode: types.PermInfo{
			Perm:         nst.mode.perm,
			LastModefied: nst.mode.lastModefied,
			CreateTime:   nst.mode.createTime,
		},
	}
	nst.RUnlock()
	ctx, h := context.WithTimeout(context.TODO(), common.ProposalTimeout)
	defer h()
	return do.NsStartCtx(ctx, log)
}

func (nst *NameSpaceTreeNode) MustUpdatePerm(do *wal.LogOpLet, path types.Path, perm types.FilePerm) error {
	log := types.NsLogImpl{
		CommandType: types.CommandUpdate,
		Path:        path,
		File: types.PersiteTreeNode{
			Name:   nst.name,
			Length: nst.length,
			Chunks: nst.chunks,
			Mode: types.PermInfo{
				Perm:         perm,
				LastModefied: nst.mode.lastModefied,
				CreateTime:   nst.mode.createTime,
			},
		},
	}

	//nst.RUnlock()
	ctx, h := context.WithTimeout(context.TODO(), common.ProposalTimeout)
	defer h()
	return do.NsStartCtx(ctx, log)
}

func (nst *NameSpaceTreeNode) MustAddChunks(do *wal.LogOpLet, path types.Path, chunks int64) error {
	//nst.RLock()

	log := types.NsLogImpl{
		CommandType: types.CommandUpdate,
		Path:        path,
		File: types.PersiteTreeNode{
			Name:   nst.name,
			Length: nst.length,
			Chunks: nst.chunks + chunks,
			Mode: types.PermInfo{
				Perm:         nst.mode.perm,
				LastModefied: nst.mode.lastModefied,
				CreateTime:   nst.mode.createTime,
			},
		},
	}

	//nst.RUnlock()
	ctx, h := context.WithTimeout(context.TODO(), common.ProposalTimeout)
	defer h()
	return do.NsStartCtx(ctx, log)
}
func (nsc *NameSpaceControlor) MustMkdir(do *wal.LogOpLet, path types.Path, newn *NameSpaceTreeNode) error {
	var children []types.PersiteTreeNode
	if newn.children != nil {
		newn.Serial(&children)
	}
	log := types.NsLogImpl{
		CommandType: types.CommandCreate | types.OP_DIC,
		Path:        path + types.Path("/"+newn.name),
		Dics:        children,
	}
	ctx, h := context.WithTimeout(context.TODO(), common.ProposalTimeout)
	defer h()
	return do.NsStartCtx(ctx, log)
}
func (nsc *NameSpaceControlor) MustDeleteFile(do *wal.LogOpLet, cwd types.Path, filename string) error {
	log := types.NsLogImpl{
		CommandType: types.CommandDelete,
		Path:        cwd,
		File: types.PersiteTreeNode{
			IsDir: false,
			Name:  filename,
		},
	}
	ctx, h := context.WithTimeout(context.TODO(), common.ProposalTimeout)
	defer h()
	return do.NsStartCtx(ctx, log)
}
func (nsc *NameSpaceControlor) MustCreateFile(do *wal.LogOpLet, cwd types.Path, filename string, perm types.FilePerm) error {
	log := types.NsLogImpl{
		CommandType: types.CommandCreate | types.OP_FILE,
		Path:        cwd,
		File: types.PersiteTreeNode{
			IsDir:  false,
			Name:   filename,
			Length: 0,
			Chunks: 0,
			Mode: types.PermInfo{
				Perm:         perm,
				LastModefied: time.Now(),
				CreateTime:   time.Now(),
			},
		},
	}
	ctx, h := context.WithTimeout(context.TODO(), common.ProposalTimeout)
	defer h()
	return do.NsStartCtx(ctx, log)
}

func (nsc *NameSpaceControlor) MustCreateBatchFile(do *wal.LogOpLet, cwd types.Path, perm types.FilePerm, filename []string) error {
	log := types.BatchLogImpl{
		Type:  types.NsLog,
		Batch: make([]interface{}, len(filename)),
	}

	for idx, v := range filename {

		log.Batch[idx] = types.NsLogImpl{
			CommandType: types.CommandCreate | types.OP_FILE,
			Path:        cwd,
			File: types.PersiteTreeNode{
				IsDir:  false,
				Name:   v,
				Length: 0,
				Chunks: 0,
				Mode: types.PermInfo{
					Perm:         perm,
					LastModefied: time.Now(),
					CreateTime:   time.Now(),
				},
			},
		}
	}

	ctx, h := context.WithTimeout(context.TODO(), common.ProposalTimeout)

	defer h()

	return do.OpStartCtx(ctx, log)
}

func (nsc *NameSpaceControlor) MustDeleteBatchFile(do *wal.LogOpLet, cwd types.Path, filename []string) error {
	log := types.BatchLogImpl{
		Type:  types.NsLog,
		Batch: make([]interface{}, len(filename)),
	}

	for idx, v := range filename {

		log.Batch[idx] = types.NsLogImpl{
			CommandType: types.CommandDelete,
			Path:        cwd,
			File: types.PersiteTreeNode{
				Name:   v,
				IsDir:  false,
				Length: 0,
				Chunks: 0,
			},
		}
	}

	ctx, h := context.WithTimeout(context.TODO(), common.ProposalTimeout)
	defer h()
	return do.OpStartCtx(ctx, log)
}

func (nsc *NameSpaceControlor) MustBatchMkdir(do *wal.LogOpLet, cwd types.Path, filename []string) error {
	log := types.BatchLogImpl{
		Type:  types.NsLog,
		Batch: make([]interface{}, len(filename)),
	}

	for idx, v := range filename {
		log.Batch[idx] = types.NsLogImpl{
			CommandType: types.CommandCreate | types.OP_DIC,
			Path:        cwd,
			Dics: []types.PersiteTreeNode{
				{
					IsDir:    false,
					Name:     v,
					Children: make([]int, 0),
				},
			},
		}
	}

	ctx, h := context.WithTimeout(context.TODO(), common.ProposalTimeout)
	defer h()
	return do.OpStartCtx(ctx, log)
}
