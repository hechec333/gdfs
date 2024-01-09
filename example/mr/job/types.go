package mr

type RequireTaskArg struct {
	X int
}
type TaskConfig struct {
	Limits  int      `json:"limits"`
	Nreduce int      `json:"nreduce"`
	Out     string   `json:"out"`
	Remotes []string `json:"remotes"`
}

type TaskStage uint8

const (
	STAGE_MAP TaskStage = iota
	STAGE_WAIT
	STAGE_REDUCE
	STAGE_DONE
)

type TaskInfo struct {
	// 0->map 1->reduce 2->phrase-wait 3->exit
	State         int
	Seq           int //序列号
	TaskId        int64
	FileName      string
	StartPos      int64 //
	EndPos        int64 // -1 代表读到尾部
	ReduceFileIdx int
	MapFileIdx    int
	// reduce num
	NReduce int
	// intermediate files num per map
	BucketsNum int
	BucketId   int

	OutPutIndex int
}

type TaskResponce struct {
	TaskInfo
	WorkerId int
}

type TaskDoneReply struct {
}
type PingArgs struct {
	X      int
	Seq    int
	Plugin string
}

type PongReply struct {
	Y         int
	Config    TaskConfig
	Driver    string
	ExecfPath string
	Execf     string
}

type CheckArg struct {
	Id         int64
	MasterAddr string
	Good       bool
	Err        string
}

type CheckReply struct {
}

type HeartBeatArg struct {
	WorkerId int
	// more metric info
}

type HeartBeatReply struct {
}

type RegisterArg struct {
	WorkerId int
	Addr     string
}

type RegisterReply struct {
}

type NewTaskArg struct {
	WorkerId   int64
	InProgress []int64
}

type NewTaskReply struct {
	MasterAddr string
	TaskId     int64
	TaskName   string
}
