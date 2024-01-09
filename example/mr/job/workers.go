package mr

import (
	"context"
	crand "crypto/rand"
	"encoding/binary"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"gdfs/types"
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
	"strings"
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
	maxPolldesc       = 1 * time.Second
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
	TaskName   string
	MasterAddr string
	Seq        int
	mapf       func(string) []KeyValue
	reducef    func(string, []string) string
	driver     StorageDriver
	History    []string
	w          *Worker
	Cfg        TaskConfig

	lastidle time.Time
	next     chan struct{}
	laststat *TaskInfo
}

func (t *TaskStat) isIdle() bool {
	if t.laststat == nil {
		return false
	}

	return t.laststat.State == int(STAGE_WAIT) || t.laststat.State == int(STAGE_DONE)
}

func (t *TaskStat) done() bool {
	return t.laststat.State == int(STAGE_DONE)
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

type JobMeta struct {
	t  *TaskInfo
	tt *TaskStat
}

func StepMapCtx(ctx context.Context, job JobMeta) error {

	dri := job.tt.driver

	f, err := dri.Open(job.t.FileName)

	if err != nil {
		return err
	}

	defer f.Close()

	pos := job.t.StartPos
	b := make([]byte, 1024*1024*16)
	result := make(chan []KeyValue, 16)

	for pos < job.t.EndPos {
		re, err := f.Read(b)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		pos += int64(re)

		go func(x []byte) {
			kv := job.tt.mapf(string(x))
			result <- kv
			x = nil
		}(b)
	}
	b = nil
	err = MakeTmpBuckets(job, result)
	if err != nil {
		return err
	}

	return nil
}

func GetJobTmpName(job JobMeta, idx int) string {
	bu := new(strings.Builder)
	binary.Write(bu, binary.BigEndian, job.t.TaskId)
	bu.Write([]byte("-"))
	binary.Write(bu, binary.BigEndian, idx)
	bu.Write([]byte("-"))
	binary.Write(bu, binary.BigEndian, job.t.MapFileIdx)
	return bu.String()
}

func CreateOutline(job JobMeta) ([]*gob.Encoder, error) {
	files := make([]io.ReadWriteCloser, job.t.NReduce)
	x := make([]*gob.Encoder, job.t.NReduce)
	var err error
	for i := 0; i < job.t.NReduce; i++ {
		ps := GetJobTmpName(job, i)
		files[i], err = job.tt.driver.Create(ps)
		if err != nil {
			return nil, err
		}
		x[i] = gob.NewEncoder(files[i])
	}
	return x, nil
}
func OpenOutline(job JobMeta) (x []*gob.Encoder, err error) {
	if job.t.Seq == 0 {
		x, err = CreateOutline(job)
		return
	}

	files := make([]io.ReadWriteCloser, job.t.NReduce)
	x = make([]*gob.Encoder, job.t.NReduce)
	for i := 0; i < job.t.NReduce; i++ {
		ps := GetJobTmpName(job, i)
		files[i], err = job.tt.driver.Open(ps)
		if err != nil {
			return nil, err
		}
		x[i] = gob.NewEncoder(files[i])
	}
	return
}
func MakeTmpBuckets(job JobMeta, src <-chan []KeyValue) error {

	x, err := OpenOutline(job)
	if err != nil {
		return err
	}

	for kvs := range src {
		for _, v := range kvs {
			idx := ihash(v.Key)
			x[idx].Encode(v)
		}
	}

	return nil
}

func StepReduceCtx(ctx context.Context, job JobMeta) error {

}

// 安装执行环境
func (t *TaskStat) InstallEnv(reply *PongReply) {
	t.Cfg = reply.Config
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
func (t *TaskStat) Step() {
	for {
		<-t.next
		xt := DoRequest(t.w.WorkerId)
		switch xt.State {
		case 0:
			ctx := context.Background()
			err := StepMapCtx(ctx, JobMeta{
				t:  xt,
				tt: t,
			})
			if err != nil {
				log.Println(err)
			}
			t.lastidle = time.Now()
			TaskDone(xt, t.w.WorkerId)
		case 1:
			ctx := context.Background()
			err := StepReduceCtx(ctx, JobMeta{
				t:  xt,
				tt: t,
			})
			if err != nil {
				log.Println(err)
			}
			t.lastidle = time.Now()
			TaskDone(xt, t.w.WorkerId)
		}

		t.laststat = xt
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

// 注册worker
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

// 向daemon申请新的task
func (w *Worker) AskNewTask() *TaskStat {
	conn, err := rpc.Dial("tcp", w.DaemonAddr)

	if err != nil {
		log.Println(err)
		return nil
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

		return &t
	}

	return nil
}

func (w *Worker) WorkerEntry() {
	w.registerSelf()
	go w.WorkerLoop()
}

func (w *Worker) WorkerLoop() {
	for {
		loopactive := 0
		for idx, v := range w.tasks {
			// 固定一段时间 poll一下挂起任务
			maxidle := v.lastidle.Add(maxPolldesc)
			if maxidle.Before(time.Now()) && v.isIdle() {
				v.next <- struct{}{}
				loopactive++
				continue
			}
			// 上一次执行到任务的本轮继续执行
			if !v.isIdle() {
				v.next <- struct{}{}
			}
			//执行完毕的任务移除
			if v.done() {
				close(v.next)
				w.tasks = append(w.tasks[:idx], w.tasks[idx+1:]...)
				break
			}
			loopactive++
		}
		//请求新任务
		if loopactive <= len(w.tasks)/2 || loopactive <= maxtaskparrel {
			st := w.AskNewTask()
			if st == nil {
				log.Println("ask new task failed")
			}
			go st.Step()
			time.Sleep(1 * time.Second)
		}
		loopactive = 0
	}

}
