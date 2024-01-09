package mr

import (
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"strconv"
	"sync"
	"time"
)

var (
	ErrTaskBusy      = errors.New("too many tasks")
	ErrDuplicateWork = errors.New("workerid has registerd")
)

var (
	timeouts = 3 * time.Second
)

type ScheduleState uint8

const (
	TASK_QUEUED ScheduleState = iota
	TASK_CHECK
	TASK_PROCESS
	TASK_FAIL
	TASK_TIMEOUT
	TASK_KILL
)

type SchedulerEntity struct {
	Task
	MasterAddr string
	State      ScheduleState
	msg        chan TaskMessage
	lastIdle   time.Time
	total      time.Duration
}

type Task struct {
	Id          int64
	TaskName    string     `json:"task_name"`
	Collections []string   `json:"collections"`
	Execfpath   string     `json:"execf_path"`
	Execf       string     `json:"execf"`
	Driver      string     `json:"driver"`
	Config      TaskConfig `json:"config"`
}

type workerStat struct {
	sync.RWMutex
	lasthb time.Time
	uuid   int64
	addr   string

	indispatch bool
	history    []Task
}

func (w *workerStat) good() bool {
	w.RLock()
	defer w.RUnlock()

	if w.indispatch {
		return time.Now().Before(w.lasthb.Add(2 * timeouts))
	}

	return time.Now().Before(w.lasthb.Add(timeouts))
}

type TaskMessage struct {
	Id            int64
	TaskName      string
	Stage         string  `json:"stage"` //map or reduce
	StageProgress float64 `json:"stage_progress"`
	TotalProgress float64 `json:"total_progress"` // 总进度
	Msg           string  `json:"msg"`
}

type Daemon struct {
	sync.RWMutex
	taskTick time.Duration
	workers  []*workerStat

	stopch chan struct{}

	taskmu  sync.RWMutex
	queuec  chan *SchedulerEntity
	activec []*SchedulerEntity
	killc   []*SchedulerEntity

	msgc chan *TaskMessage
}

func (d *Daemon) StartMrDaemon(addr string) {
	go d.KeepAliveWorkers()
	go d.RpcServer(addr)
}

func (d *Daemon) KeepAliveWorkers() {
	for {
		select {
		case <-d.stopch:
		case <-time.After(200 * time.Millisecond):
		}
		for i, v := range d.workers {
			if !v.good() {
				d.Lock()
				d.workers = append(d.workers[:i], d.workers[i+1:]...)
				d.Unlock()
				d.ResumeWorker()
				break
			}
		}
	}
}

func (d *Daemon) RpcServer(addr string) {
	s := rpc.NewServer()

	s.Register(d)

	l, err := net.Listen("tcp", addr)

	if err != nil {
		panic(err)
	}

	for {
		select {
		case <-d.stopch:
		default:
		}
		conn, err := l.Accept()

		if err != nil {
			log.Println(err)
			continue
		}

		go s.ServeConn(conn)
	}

}

func (d *Daemon) TaskMonitor() {
	t := time.NewTicker(d.taskTick)
	for {
		select {
		case t := <-d.queuec:
			d.StartTask(t) //启动任务
		case m := <-d.msgc:
			d.DispatchMsg(m) //处理处理过程消息
		case <-t.C:
			d.killTask() //移除超时任务
		}
	}
}

func (d *Daemon) killTask() {
	d.RLock()
	defer d.RUnlock()
	sl := []*SchedulerEntity{}
retry:
	for i, v := range d.activec {
		if v.total >= time.Duration(v.Config.Limits) {
			v.State = TASK_TIMEOUT
			d.killc = append(d.killc, v)
			d.activec = append(d.activec[:i], d.activec[i+1:]...)
			goto retry
		}
		sl = append(sl, v)
	}

	for _, v := range sl {
		v.total += d.taskTick
	}
}

func (d *Daemon) DispatchMsg(m *TaskMessage) {
	d.RLock()
	defer d.RUnlock()
	for _, v := range d.activec {
		if v.Id == m.Id {
			select {
			case v.msg <- *m:
				v.lastIdle = time.Now()
			default:
				break
			}
		}
	}
}

func (d *Daemon) StartTask(t *SchedulerEntity) {
	d.StartTaskMaster(t)
	t.Id = Nrand()
	t.State = TASK_CHECK

	d.taskmu.Lock()
	defer d.taskmu.Unlock()

	d.activec = append(d.activec, t)
}

func (d *Daemon) handleTaskQueue(t *Task) error {
	n := SchedulerEntity{
		Task:  *t,
		State: TASK_QUEUED,
	}
	if t.Config.Limits == 0 {
		t.Config.Limits = int(time.Second * 180)
	}
	select {
	case d.queuec <- &n:
		return nil
	default:
		return ErrTaskBusy
	}
}

// api 路由层
func (d *Daemon) registerRoutes() {
	//
	http.HandleFunc("/task/start", func(w http.ResponseWriter, r *http.Request) {
		res := struct {
			Good  bool   `json:"state"`
			Uid   string `json:"uid"`
			Error string `json:"msg"`
		}{}
		defer r.Body.Close()

		defer func() {
			b, err := json.Marshal(res)

			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusOK)
			w.Write(b)
		}()

		w.Header().Add("Contet-Type", "application/json")
		if r.Method != "POST" {
			w.WriteHeader(http.StatusBadRequest)
			res.Good = false
			res.Error = "not accpeted method,try POST"
			return
		}

		bs, err := io.ReadAll(r.Body)

		if err != nil {
			res.Error = err.Error()
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		task := Task{}

		json.Unmarshal(bs, &task)

		err = d.handleTaskQueue(&task)

		if err != nil {
			res.Error = err.Error()
			res.Good = false
			return
		}
		res.Good = true
		res.Uid = strconv.FormatInt(task.Id, 10)
	})

	http.HandleFunc("/task/stat", func(w http.ResponseWriter, r *http.Request) {
		res := struct {
			TaskName  string        `json:"task_name"`
			LastIdle  time.Time     `json:"last_idle"`
			TotalTime time.Duration `json:"total_time"`
			Message   []TaskMessage `json:"message"`
		}{}

		defer r.Body.Close()

		if r.Method != "GET" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		var (
			l     = r.URL.Query()
			pages = -1
			id    = -1
			err   error
		)

		if xp := l.Get("pages"); xp != "" {
			pages, err = strconv.Atoi(xp)
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
		}

		if xp := l.Get("id"); xp != "" {
			id, err = strconv.Atoi(xp)
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
		}

		d.RLock()
		defer d.RUnlock()
		for _, v := range d.activec {
			if v.Id == int64(id) {
				res.LastIdle = v.lastIdle
				res.TaskName = v.TaskName
				res.TotalTime = v.total

				if pages == -1 {
					pages = cap(v.msg)

				}
				for i := 0; i < pages; i++ {
					res.Message = append(res.Message, <-v.msg)
				}
				break
			}
		}

		if res.TaskName == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		b, _ := json.Marshal(res)
		w.WriteHeader(http.StatusOK)
		w.Write(b)
	})
}

func (d *Daemon) ApiServer(addr string) error {

	d.registerRoutes()

	return http.ListenAndServe(addr, nil)
}

func (d *Daemon) ResumeWorker() {

}

type MasterSetupArg struct {
	Task
	DaemonRPCAddr string
}

// 请求新的task执行
func (d *Daemon) RPCAskNewTask(arg *NewTaskArg, reply *NewTaskReply) error {

	d.taskmu.Lock()
	defer d.taskmu.Unlock()

	for _, v := range d.activec {
		if v.State == TASK_PROCESS {
			var f bool = true
			for _, vv := range arg.InProgress {
				if vv == v.Id {
					f = false
				}
			}
			if f {
				reply.TaskId = v.Id
				reply.TaskName = v.TaskName
				reply.MasterAddr = v.MasterAddr
				break
			}
		}
	}
	return nil
}

// 启动注册worker
func (d *Daemon) RPCRegister(arg *RegisterArg, reply *RegisterReply) error {
	d.Lock()
	defer d.Unlock()

	for _, v := range d.workers {
		if v.uuid == int64(arg.WorkerId) {
			return ErrDuplicateWork
		}
	}
	d.workers = append(d.workers, &workerStat{
		uuid:       int64(arg.WorkerId),
		addr:       arg.Addr,
		lasthb:     time.Now(),
		indispatch: false,
		history:    make([]Task, 0),
	})
	return nil
}

// Worker心跳
func (d *Daemon) RPCHeartBeat(arg *HeartBeatArg, reply HeartBeatReply) error {

	return nil
}

// job启动阶段的校验结果
func (d *Daemon) RPCCheck(arg *CheckArg, reply *CheckReply) error {

	d.Lock()
	defer d.Unlock()

	for i, v := range d.activec {
		if v.Id == arg.Id && v.State == TASK_CHECK {
			if arg.Good {
				v.State = TASK_PROCESS
				v.MasterAddr = arg.MasterAddr
				v.msg = make(chan TaskMessage, 128)
				v.lastIdle = time.Now()
				break
			}
			//检测失败
			d.activec = append(d.activec[:i], d.activec[i+1:]...)
			v.State = TASK_KILL
			d.killc = append(d.killc, v)
			return nil
		}
	}

	return nil
}

// 正常这个应该启动一个进程来代理Task
// 分布式环境下可以给将Master分配到远程环境中
func (d *Daemon) StartTaskMaster(t *SchedulerEntity) error {

	go func() {
		arg := MasterSetupArg{
			Task: t.Task,
		}
		MasterEntrypoint(arg)
	}()

	return nil
}
