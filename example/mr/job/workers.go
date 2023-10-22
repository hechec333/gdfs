package mr

import (
	crand "crypto/rand"
	"encoding/json"
	"fmt"
	"gdfs/internal/types"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"math/big"
	"net/rpc"
	"os"
	"path"
	"path/filepath"
	"plugin"
	"sort"
	"strconv"
	"sync"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

var (
	maxtaskparrel int = 2
)

type kvSlice []KeyValue

func (s kvSlice) Len() int           { return len(s) }
func (s kvSlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s kvSlice) Less(i, j int) bool { return s[i].Key < s[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var moduelName = ""

type TaskStat struct {
	TaskId     int64
	MasterAddr string
	Seq        int
	mapf       func(string, string) []KeyValue
	reducef    func(string, []string) string
	driver     StorageDriver
	History    []string
	w          *Worker
}

type Worker struct {
	sync.RWMutex
	WorkerId   int
	tasks      []TaskStat
	Addr       string
	DaemonAddr string
}

// start a normal worker
func Start(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string, module string) {
	Max := big.NewInt(0xffffffff)
	moduelName = module
	uniqueNumber, _ := crand.Int(crand.Reader, Max)
	// Your worker implementation here.
	go func() {
		time := time.NewTicker(time.Second * 1)
		for {
			if CallPing(uniqueNumber.Int64()) { // accpet the quit message
				time.Stop()
			}
			<-time.C
		}
	}()
	time.Sleep(time.Millisecond * 500)
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	// logic-statement @map-reduce-worker
	// 1.ask a request from master
	// 2.judge the @TaskInfo.state to confirm which routine to excuete
	// @executeMap state = 0
	// 		1.according the @TaskInfo from master,create @Taskinfo.nReduce times of tmp-file
	// 			be noticed the tmp-file is used to store the ir-kv and it's indepedent acorss the mapper,each mapper
	//		  has it's own bucket-number,like @inbucketId=2 (ps asuume @Taskinfo.nReduce=10)
	// 			means all the tmp-files name-sequence likes mr-tmp-0...9-2
	//		2..according the @TaskInfo.FileName read the file to strings
	//		3.excute mapf to produce the ir-kv
	// 		4.store all the ir-kv into the tmp-file pass the ihash(k) to select a bucket
	//    5.according the execution to make a responce to master
	// @executeReduce state = 1
	//   1.according the @Taskinfo.nReduce and @TaskInfo.inbucketId to read the all in-file ir-kv
	//     ps: according the map staet all the same @key's ir-kv will put in the mr-tmp-
	//   2.sort all the ir-kv
	//   3.merge all the ir-kv to a single string compare value array
	//   4.excute the reducef
	//   5.make the ouput in the mr-out-@TaskInfo.OutPutIdx
	// @executeSleep state = 2
	// @executeExit state = 3
	for {
		task := DoRequest(int(uniqueNumber.Int64()))
		beg := time.Now()
		switch task.State {
		case 0: // Map
			execMapper(mapf, task, int(uniqueNumber.Int64()))
			fmt.Println("Map stage", task.MapFileIdx, "--cost:", time.Since(beg))
		case 1:
			execReducer(reducef, task, int(uniqueNumber.Int64()))
			fmt.Println("Reduce stage", task.MapFileIdx, "--cost:", time.Since(beg))
		case 2:
			time.Sleep(time.Second)
			fmt.Println("wait for work...")
		case 3:
			fmt.Println("all of task have been done")
			return
		default:
			fmt.Println("noknown state number:", task.State, " program exit")
			os.Exit(-1)
		}
	}

}
func execMapper(mapf func(string, string) []KeyValue, t *TaskInfo, id int) {
	// 1.according the @TaskInfo from master,create @Taskinfo.nReduce times of tmp-file
	// 			be noticed the tmp-file is used to store the ir-kv and it's indepedent acorss the mapper,each mapper
	//		  has it's own bucket-number,like @inbucketId=2 (ps asuume @Taskinfo.nReduce=10)
	// 			means all the tmp-files name-sequence likes mr-tmp-0...9-2
	//		2..according the @TaskInfo.FileName read the file to strings
	//		3.excute mapf to produce the ir-kv
	// 		4.store all the ir-kv into the tmp-file pass the ihash(k) to select a bucket
	//    5.according the execution to make a responce to master

	//@1
	ifile, err := os.Open(t.FileName)
	defer func() {
		err := ifile.Close()
		if err != nil {
			panic(err)
		}
	}()

	if err != nil {
		panic(err)
	}
	//@2
	content, err := ioutil.ReadAll(ifile)

	if err != nil {
		panic(err)
	}

	innerValue := []KeyValue{}

	//@3
	result := mapf(t.FileName, string(content))

	innerValue = append(innerValue, result...)

	//@4
	oerr := makeIntermediateStorage(&innerValue, t)
	if oerr != nil {
		panic(oerr)
	}
	TaskDone(t, id)
}
func makeIntermediateStorage(vv *[]KeyValue, t *TaskInfo) error {
	tfiles := make([]*os.File, t.NReduce)
	tfilesEncoder := make([]*json.Encoder, t.NReduce)
	errs := make([]error, t.NReduce)
	for idx := 0; idx < t.NReduce; idx++ {
		tfiles[idx], errs[idx] = ioutil.TempFile("", "mr-*")
		tfilesEncoder[idx] = json.NewEncoder(tfiles[idx])
	}
	var err error = nil
	for _, v := range *vv {
		tfilesEncoder[ihash(v.Key)%t.NReduce].Encode(v)
	}
	outPrfix := "/tmp/mr-"
	// mr-0...n-2
	outSufix := "-" + strconv.Itoa(t.MapFileIdx)
	for i, v := range tfiles {
		oldPath := filepath.Join(v.Name())
		err = os.Rename(oldPath, outPrfix+strconv.Itoa(i)+outSufix)
	}
	return err
}
func getIntetmediateStorage(vv *[]KeyValue, t *TaskInfo) error {
	tmpPathPrefix := "/tmp/mr-" + strconv.Itoa(t.ReduceFileIdx)
	for idx := 0; idx < t.BucketsNum; idx++ {
		file, err := os.Open(tmpPathPrefix + "-" + strconv.Itoa(idx))
		if err != nil {
			fmt.Printf("open intermediate file %v failed,what:%v", file.Name(), err)
			return err
		}
		dec := json.NewDecoder(file)
		for {
			kv := KeyValue{}
			if err := dec.Decode(&kv); err != nil {
				break
			}
			*vv = append(*vv, kv)
		}
		file.Close()
	}
	return nil
}
func execReducer(reducef func(string, []string) string, t *TaskInfo, id int) {
	//@executeReduce state = 1
	//   1.according the @Taskinfo.nReduce and @TaskInfo.inbucketId to read the all in-file ir-kv
	//     ps: according the map staet all the same @key's ir-kv will put in the mr-tmp-
	//   2.sort all the ir-kv
	//   3.merge all the ir-kv to a single string compare value array
	//   4.excute the reducef
	//   5.make the ouput in the mr-out-@TaskInfo.OutPutIdx
	arr := []KeyValue{}
	err := getIntetmediateStorage(&arr, t)
	if err != nil {
		panic(err)
	}
	var file *os.File
	outputPath := "mr-out-" + strconv.Itoa(t.ReduceFileIdx)
	//file, err = os.Create(outputPath)
	if _, ferr := os.Stat(outputPath); ferr == nil {
		file, _ = os.OpenFile(outputPath, os.O_WRONLY|os.O_APPEND, 0666)
	} else {
		file, err = os.Create(outputPath)
	}
	if err != nil {
		panic(err)
	}
	var str []byte
	sort.Sort(kvSlice(arr))
	i := 0
	for i < len(arr) {
		j := i + 1
		for j < len(arr) && arr[j].Key == arr[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, arr[k].Value)
		}
		output := reducef(arr[i].Key, values)
		//fmt.Fprintf(file, "%v %v\n", arr[i].Key, output)
		str = append(str, fmt.Sprintf("%v %v\n", arr[i].Key, output)...)
		i = j
	}
	file.Write(str)
	TaskDone(t, id)
	defer file.Close()
}

// 安装执行环境
func (t *TaskStat) InstallEnv(reply *PongReply) {

	if reply.Driver == "local" {
		t.driver = NewLocalDriver()
	}
	if reply.Driver == "remote" {
		addrs := []types.Addr{}
		for _, v := range reply.Config.Remotes {
			addrs = append(addrs, types.Addr(v))
		}
		t.driver = NewGdfsStorageDriver(addrs)
	}
	px := path.Join(reply.ExecfPath, reply.Execf)
	f, err := t.driver.Open(px)

	if err != nil {
		log.Println(err) // 严重的执行错误，应该向daemon恢复
		return
	}

	xf, _ := os.Create(reply.Execf)

	_, err = io.Copy(xf, f)

	if err != nil {
		log.Println(err)
		return
	}

	t.mapf, t.reducef, err = loadPlugin(reply.Execf)

	if err != nil {
		log.Println(err)
		return
	}
}

// 执行循环
func (t *TaskStat) Step(xt *TaskInfo) {
	beg := time.Now()
	switch xt.State {
	case 0:
		execMapper(t.mapf, xt, t.w.WorkerId)
		fmt.Println("Map stage", xt.MapFileIdx, "--cost:", time.Since(beg))
	case 1:
		execReducer(t.reducef, xt, t.w.WorkerId)
		fmt.Println("Reduce stage", xt.MapFileIdx, "--cost:", time.Since(beg))
	}
}

func (t *TaskStat) mapper() {

}

// 加载动态库
func loadPlugin(filename string) (func(string, string) []KeyValue, func(string, []string) string, error) {
	p, err := plugin.Open(filename)
	if err != nil {
		return nil, nil, err
	}
	xmapf, err := p.Lookup("Map")
	if err != nil {
		return nil, nil, err
	}
	mapf := xmapf.(func(string, string) []KeyValue)
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		return nil, nil, err
	}
	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef, nil
}
func (t *TaskStat) CallPing(number int64) bool {
	args := PingArgs{
		Seq: t.Seq,
		X:   int(number),
	}
	reply := PongReply{}

	call(t.MasterAddr, "Coordinator.Ping", &args, &reply)

	if t.Seq == 0 {
		t.InstallEnv(&reply)
	}

	return reply.Y != 0
}

// example function to show how to make an RPC call to the coordinator.

func DoRequest(id int) *TaskInfo {
	args := RequireTaskArg{
		X: id,
	}
	TaskInfo := TaskInfo{}

	call("Coordinator.RequireTask", &args, &TaskInfo)

	//fmt.Println("gain Task")

	return &TaskInfo
}

func TaskDone(t *TaskInfo, id int) {
	info := TaskResponce{
		TaskInfo: *t,
		WorkerId: id,
	}
	reply := TaskDoneReply{}

	call("Coordinator.TaskDone", &info, &reply)
}

func call(addr string, rpcname string, args interface{}, reply interface{}) bool {
	conn, err := rpc.Dial("tcp", addr)

	if err != nil {
		log.Println(err)
	}

	err = conn.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func (w *Worker) registerSelf() {

	conn, err := rpc.Dial("tcp", w.DaemonAddr)

	if err != nil {
		log.Println(err)
		return
	}

	arg := RegisterArg{
		WorkerId: w.WorkerId,
		Addr:     w.Addr,
	}

	reply := RegisterReply{}

	err = conn.Call("RPCRegister", &arg, &reply)

	if err != nil {
		log.Println(err)
	}
}

func (w *Worker) AskNewTask() {
	conn, err := rpc.Dial("tcp", w.DaemonAddr)

	if err != nil {
		log.Println(err)
		return
	}
	inprogress := []int64{}
	for _, v := range w.tasks {
		inprogress = append(inprogress, v.TaskId)
	}

	arg := NewTaskArg{
		WorkerId:   int64(w.WorkerId),
		InProgress: inprogress,
	}
	reply := NewTaskReply{}
	conn.Call("Daemon.RPCAskNewTask", &arg, &reply)

	if reply.TaskId != 0 {
		t := TaskStat{
			TaskId:     reply.TaskId,
			MasterAddr: reply.MasterAddr,
			Seq:        0,
		}
		t.CallPing(int64(w.WorkerId)) //第一次ping获取执行信息
		w.tasks = append(w.tasks, t)
	}
}
