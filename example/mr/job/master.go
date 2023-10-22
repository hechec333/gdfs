package mr

import (
	"fmt"
	"gdfs/internal/types"
	"log"
	"net"
	"net/rpc"
	"os"
	"os/exec"
	"path"
	"sync"
	"time"
)

var (
	OUT_OF_TIME            = 5 // max lose heartbeat times
	MAX_OVER_LAP           = 2
	MAX_OF_PING_TIME       = 3
	MAX_PRIEOD_PER_CHECKED = 3
	MAX_CONCURRETNCY       = 4
)

type TaskState struct {
	Filename      string
	ReduceFileIdx int
	MapFileIdx    int
	// reduce num
	NReduce int
	// intermediate files num per map
	BucketsNum int
	NotifyFlag bool

	Start time.Time
}

func New(filename string, rindex int, mindex int, nreduce int, buckets int) TaskState {
	return TaskState{
		Filename:      filename,
		ReduceFileIdx: rindex,
		MapFileIdx:    mindex,
		NReduce:       nreduce,
		BucketsNum:    buckets,
		NotifyFlag:    false,
	}
}
func (f *TaskState) SetTime() {
	f.Start = time.Now()
}
func (f *TaskState) OutOfTime() bool {
	return int(time.Since(f.Start).Seconds()) > OUT_OF_TIME
}

type MapperState struct {
	TaskState
}

func (f *MapperState) NewTask() TaskInfo {
	f.SetTime()
	return TaskInfo{
		FileName:      f.Filename,
		State:         0,
		ReduceFileIdx: 0,
		MapFileIdx:    f.MapFileIdx,
		NReduce:       f.NReduce,
		BucketsNum:    f.BucketsNum,
	}
}

type ReducerState struct {
	TaskState
}

func (f *ReducerState) NewTask() TaskInfo {
	f.SetTime()
	return TaskInfo{
		FileName:      f.Filename,
		State:         1,
		ReduceFileIdx: f.ReduceFileIdx,
		MapFileIdx:    0,
		NReduce:       f.NReduce,
		BucketsNum:    f.BucketsNum,
	}
}

// wait queue
type WaitSignals struct {
	isWait       bool
	state        TaskState
	overLapTimes int
	id           int
}

// ping request from worker
type PingPong struct {
	id     int
	Last   time.Time
	Plugin string
}

func NewPingPong(id int, p string) *PingPong {
	return &PingPong{
		id:     id,
		Last:   time.Now(),
		Plugin: p,
	}
}

func (p *PingPong) Refresh() {
	p.Last = time.Now()
}
func (p *PingPong) OutOfTime() bool {
	return time.Since(p.Last) > time.Duration(MAX_OF_PING_TIME*int(time.Second))
}

type Detecher struct {
	pingUnit map[int]*PingPong //这里必须使用指针,map的值如果不是指针则是不可修改的,那么ping函数不能修改ping的修改时间
	l        *log.Logger
	m        sync.Mutex
}

func NewDetecher() *Detecher {
	file, _ := os.Create("./" + time.Now().Format("15:04:05") + ".log")
	return &Detecher{
		pingUnit: make(map[int]*PingPong),
		l:        log.New(file, "TRACE:", log.Flags()),
	}
}
func (d *Detecher) OutOfPing(id int) bool {
	item, err := d.pingUnit[id]
	if !err {
		return true
	}
	d.m.Lock()
	defer d.m.Unlock()
	return item.OutOfTime()
}

// check if the worker ping in time or not ,if not resume a worker
func (d *Detecher) Check() {
	d.m.Lock()
	for k, v := range d.pingUnit {
		if v.OutOfTime() {
			d.ResumeWorker(v)
			delete(d.pingUnit, k)
			break
		}
	}
	defer d.m.Unlock()
}
func (d *Detecher) ResumeWorker(u *PingPong) {
	fmt.Println("[Tracker] Detected Worker lost Ping,try to Resume New Worker")
	// if len(d.pingUnit) > MAX_CONCURRETNCY {
	// 	fmt.Println("[Tracker] MAX_CONCURTNCY")
	// 	return
	// }
	// if len(d.pingUnit) <= MAX_CONCURRETNCY/2 { //提升并发性
	// 	for i := 0; i <= MAX_CONCURRETNCY/2-len(d.pingUnit); i++ {
	// 		RestartWork(d)
	// 	}
	// } else {
	// 	RestartWork(d)
	// }

}
func RestartWork(d *Detecher) error {
	cmd := exec.Command("/bin/bash", "resumer.sh")
	cmd.Stdout = d.l.Writer()
	cmd.Stderr = d.l.Writer()
	err := cmd.Start()
	if err != nil {
		fmt.Println("failed to resume worker ", err)
	} else {
		fmt.Println("[Tracker] sucess to resume worker")
	}
	return err
}

type Tracker struct {
	owner       *Coordinator
	ticker      *time.Ticker
	PingHandler *Detecher
}

func (t *Tracker) Stop() {
	t.ticker.Stop()
}

// 开始监控每个等待队列的情况，如果等待过久，就重新启动任务
func (t *Tracker) Start() {
	t.ticker = time.NewTicker(time.Duration(time.Second * time.Duration(MAX_PRIEOD_PER_CHECKED)))
	go func() {
		for {
			times := <-t.ticker.C
			fmt.Println("[Tracker] check", times)
			t.owner.Mutex.Lock()
			switch t.owner.State {
			case 0:
			M:
				for k, v := range t.owner.mapperWaitChannel {
					if v.isWait && v.state.OutOfTime() {
						if !t.PingHandler.OutOfPing(v.id) { //可能存在一种情况，一个任务需要执行很久，但是ping正常
							v.state.SetTime()
							continue
						}
						fmt.Println("[FATAL] task :[", v.state.MapFileIdx, "] out of time,dispatch to worker", v.id)
						fmt.Println("[Tracker] worker id:", v.id)
						t.resumeTask(&v.state)
						t.owner.mapperWaitChannel = append(t.owner.mapperWaitChannel[:k], t.owner.mapperWaitChannel[k+1:]...)
						t.PingHandler.Check()
						break M
					}
				}
			case 1:
			T:
				for k, v := range t.owner.reducerWaitChannel {
					if v.isWait && v.state.OutOfTime() {
						if !t.PingHandler.OutOfPing(v.id) { //可能存在一种情况，一个任务需要执行很久，但是ping正常
							v.state.SetTime()
							continue
						}
						fmt.Println("[FATAL] task :[", v.state.ReduceFileIdx, "] out of time,dispatch to worker", v.id)
						fmt.Println("[Tracker] worker id:", v.id)
						t.resumeTask(&v.state)
						t.owner.reducerWaitChannel = append(t.owner.reducerWaitChannel[:k], t.owner.reducerWaitChannel[k+1:]...)
						t.PingHandler.Check()
						break T
					}
				}
			}
			t.owner.Mutex.Unlock()
		}
	}()
}
func (t *Tracker) resumeTask(task *TaskState) {
	task.SetTime()
	if t.owner.State == 0 {
		t.owner.mapperReadyChannel <- MapperState{
			TaskState: *task,
		}
	} else if t.owner.State == 1 {
		t.owner.reducerReadyChannel <- ReducerState{
			TaskState: *task,
		}
	}
}

type Coordinator struct {
	// Your definitions here.
	fileLists           []string
	BucketsNum          int
	NReduce             int
	DoneWaitChannel     chan bool
	State               int
	CurrentExecIndex    int
	mapperReadyChannel  chan MapperState
	mapperWaitChannel   []WaitSignals
	reducerReadyChannel chan ReducerState
	reducerWaitChannel  []WaitSignals
	T                   *Tracker
	Mutex               sync.Mutex
}

func (c *Coordinator) Ping(args *PingArgs, reply *PongReply) error {
	c.T.PingHandler.m.Lock()
	if _, ok := c.T.PingHandler.pingUnit[args.X]; !ok {
		c.T.PingHandler.pingUnit[args.X] = NewPingPong(args.X, args.Plugin)
	} else {
		c.T.PingHandler.pingUnit[args.X].Refresh()
	}
	reply.Y = 0
	//fmt.Println("[PING] Ping from worker:", args.X)
	c.T.PingHandler.m.Unlock()
	return nil
}

// worker pull a task from master
// master store a @TaskState to desicrible this task

func (c *Coordinator) RequireTask(args *RequireTaskArg, reply *TaskInfo) error {
	// prevent date-race from @taskdone
	if len(c.DoneWaitChannel) != 0 {
		reply.State = 3
		return nil
	}
	c.Mutex.Lock()
	// 开启监控线程
	if c.T.ticker == nil {
		c.T.Start()
	}

	switch c.State {
	case 0:
		if len(c.mapperReadyChannel) == 0 { // if empty let worker sleep
			reply.State = 2
		} else {
			mapper := <-c.mapperReadyChannel
			mapper.SetTime()
			*reply = mapper.NewTask()
			c.mapperWaitChannel = append(c.mapperWaitChannel, WaitSignals{
				id:           args.X,
				isWait:       true,
				state:        mapper.TaskState,
				overLapTimes: 0,
			})
			fmt.Println("[dispatch] @map-task id:", reply.MapFileIdx, "on worker :", args.X)
		}
	case 1:
		if len(c.reducerReadyChannel) == 0 {
			reply.State = 2
		} else {
			reducer := <-c.reducerReadyChannel
			reducer.SetTime()
			*reply = reducer.NewTask()
			c.reducerWaitChannel = append(c.reducerWaitChannel, WaitSignals{
				id:           args.X,
				isWait:       true,
				state:        reducer.TaskState,
				overLapTimes: 0,
			})
			fmt.Println("[dispatch] @reduce-task id:", reply.ReduceFileIdx, "on worker :", args.X)
		}
	default:
		fmt.Println("unknown state", c.State)
		os.Exit(0)
	}
	defer c.Mutex.Unlock()
	return nil
}

// 选取一个reduce output number
func (c *Coordinator) Schedule() int {
	if c.CurrentExecIndex+1 >= c.NReduce {
		c.CurrentExecIndex = c.CurrentExecIndex + 1%c.NReduce
	} else {
		c.CurrentExecIndex++
	}
	return c.CurrentExecIndex
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(l net.Listener) {
	//rpc.Register(c)
	s := rpc.NewServer()
	s.Register(c)
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				fmt.Println(err)
				continue
			}
			go s.ServeConn(conn)
		}
	}()
}
func (c *Coordinator) TaskDone(args *TaskResponce, reply *TaskDoneReply) error {
	c.Mutex.Lock()
	switch c.State {
	case 0:
		for k, v := range c.mapperWaitChannel {
			if v.isWait && v.id == args.WorkerId {
				v.isWait = false
				if len(c.mapperReadyChannel) == 0 && len(c.mapperWaitChannel) == 1 { //最后一个等待任务到达
					c.State = 1
					c.initReduceState()
				}
				c.mapperWaitChannel = append(c.mapperWaitChannel[:k], c.mapperWaitChannel[k+1:]...)
				fmt.Printf("[DONE] mapperId:%d,worker id:%d\n", v.state.MapFileIdx, v.id)
				break
			}
		}
	case 1:
		for k, v := range c.reducerWaitChannel {
			if v.isWait && v.id == args.WorkerId {
				v.isWait = false
				if len(c.reducerReadyChannel) == 0 && len(c.reducerWaitChannel) == 1 {
					c.DoneWaitChannel <- true
				}
				c.reducerWaitChannel = append(c.reducerWaitChannel[:k], c.reducerWaitChannel[k+1:]...)
				fmt.Printf("[DONE] reducerId:%d,worker id:%v\n", v.state.ReduceFileIdx, v.id)
				break
			}
		}
	}
	defer c.Mutex.Unlock()
	return nil
}
func (c *Coordinator) initReduceState() {
	fmt.Println("init reducer")
	for i := 0; i < c.NReduce; i++ {
		c.reducerReadyChannel <- ReducerState{
			TaskState: New("", i, 0, c.NReduce, c.BucketsNum),
		}
	}
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	//ret := false

	// Your code here.
	return len(c.DoneWaitChannel) == 1
}

// create a Coordinator.
// main/mrcoordinator.go calls f function.
// nReduce is the number of reduce tasks to use.Meanwhile nReduce is the number of output file mr-out-0..nreduce

func MakeCoordinator(files []string, l net.Listener, m MasterSetupArg) *Coordinator {
	c := Coordinator{}
	c.NReduce = m.Config.Nreduce
	c.BucketsNum = len(files)
	// Your code here.
	c.fileLists = append(c.fileLists, files...)
	c.DoneWaitChannel = make(chan bool, 1)
	c.State = 0
	c.CurrentExecIndex = 0
	c.mapperReadyChannel = make(chan MapperState, len(files))
	c.reducerReadyChannel = make(chan ReducerState, c.NReduce)
	c.T = &Tracker{
		owner:       &c,
		ticker:      nil,
		PingHandler: NewDetecher(),
	}
	for index, v := range files {
		c.mapperReadyChannel <- MapperState{
			TaskState: New(v, 0, index, c.NReduce, c.BucketsNum),
		}
	}
	c.server(l)
	return &c
}

func reportCheck(arg CheckArg) {
	
}

func MasterEntrypoint(m MasterSetupArg) {
	var (
		err error
		arg CheckArg
	)
	defer reportCheck(arg)
	l, err := net.Listen("tcp", "127.0.0.1:0")

	if err != nil {
		arg.Good = false
		arg.Err = err.Error()
		return
	}
	arg.Good = true
	if !check(m, &arg) {
		return
	}
	//t.MasterAddr = l.Addr().String()

	arg.MasterAddr = l.Addr().String()

	MakeCoordinator(m.Collections, l, m)
}

func check(m MasterSetupArg, arg *CheckArg) bool {
	var path = path.Join(m.Execfpath, m.Execf)
	if m.Driver == "local" {
		ld := NewLocalDriver()
		if !ld.Exist(path) {
			arg.Err = "not found local driver"
			arg.Good = false
			return false
		}
		for _, v := range m.Collections {
			if !ld.Exist(v) {
				arg.Good = false
				arg.Err = fmt.Sprintf("not found entries file %v;", v)
			}
		}

		return arg.Good
	}

	if m.Driver == "remote" {
		addrs := []types.Addr{}
		for _, v := range m.Config.Remotes {
			addrs = append(addrs, types.Addr(v))
		}
		ld := NewGdfsStorageDriver(addrs)

		if !ld.Exist(path) {
			arg.Err = "not found remote driver"
			arg.Good = false
			return false
		}

		for _, v := range m.Collections {
			if !ld.Exist(v) {
				arg.Good = false
				arg.Err = fmt.Sprintf("not found entries file %v;", v)
			}
		}

		return arg.Good
	}

	arg.Err = "not support driver type"
	return false
}
