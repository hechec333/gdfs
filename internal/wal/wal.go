package wal

import (
	"bytes"
	"context"
	"encoding/gob"
	"gdfs/internal/common"
	"gdfs/internal/common/rpc"
	"gdfs/internal/wal/raft"
	"gdfs/types"
	"log"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

func init() {
	gob.Register(LogOp{})
	gob.Register(InvalidApplyMsg{})
}

type ICommandLet interface {
	ServeApplyCommand(raft.ApplyMsg) (error, error) //
	ServeSnapShot() ([]byte, error)
	InstallSnapShot([]byte) error
}

type LogOp struct {
	ClientId int64
	Seq      int64
	Region   string // "client" or "internal"
	SSeq     int64
	OpType   int
	Key      string
	Log      interface{}
}

type WaitResult struct {
	Err  error
	Term int
}

type SeqResponce struct {
	Seq  int64
	SSeq int64
	Resp WaitResult
}
type WriteAheadLog struct {
	mu             sync.Mutex
	me             int
	rf             *raft.Raft
	dead           int32
	maxraftstate   int
	commitCh       map[int]chan WaitResult
	clientMap      map[int64]SeqResponce
	applyCh        chan raft.ApplyMsg
	cmd            ICommandLet
	LastApplyIndex int

	// non-volatite

	rsu         sync.RWMutex
	readindex   int
	readOpinit  int
	readinitsem *sync.Cond
	rolechange  <-chan raft.Event
}

type LogOpLet struct {
	wal      *WriteAheadLog
	ClientId int64
	Seq      int64
	SSeq     int64
	Region   string
}

func NewLogOpLet(wal *WriteAheadLog, ClientId, Seq int64, region string) *LogOpLet {
	return &LogOpLet{
		wal:      wal,
		ClientId: ClientId,
		Seq:      Seq,
		SSeq:     1,
		Region:   region,
	}
}

func (lol *LogOpLet) ChunkStartCtx(ctx context.Context, cmd interface{}) error {

	defer func() {
		lol.SSeq++
	}()
	log := LogOp{
		ClientId: lol.ClientId,
		Seq:      lol.Seq,
		SSeq:     lol.SSeq,
		Region:   lol.Region,
		OpType:   types.ChunkLog,
		Log:      cmd,
	}
	ch := make(chan error)
	go func() {
		if err := lol.wal.startCommand(lol.ClientId, lol.Seq, log); err != nil {
			ch <- err
		}
		close(ch)
	}()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-ch:
			return err
		}
	}
}

func (lol *LogOpLet) OpStartCtx(ctx context.Context, cmd interface{}) error {
	defer func() {
		lol.SSeq++
	}()

	log := LogOp{
		ClientId: lol.ClientId,
		Seq:      lol.Seq,
		SSeq:     lol.SSeq,
		Region:   lol.Region,
		OpType:   types.BatchLog,
		Log:      cmd,
	}

	ch := make(chan error)

	go func() {
		if err := lol.wal.startCommand(lol.ClientId, lol.Seq, log); err != nil {
			ch <- err
		}
		close(ch)
	}()

	for {
		select {
		case <-ctx.Done(): //context dealine execeed!
			return ctx.Err()
		case err := <-ch:
			return err
		}
	}
}

func (lol *LogOpLet) NsStartCtx(ctx context.Context, cmd interface{}) error {

	defer func() {
		lol.SSeq++
	}()

	log := LogOp{
		ClientId: lol.ClientId,
		Seq:      lol.Seq,
		SSeq:     lol.SSeq,
		Region:   lol.Region,
		OpType:   types.NsLog,
		Log:      cmd,
	}

	ch := make(chan error)

	go func() {
		if err := lol.wal.startCommand(lol.ClientId, lol.Seq, log); err != nil {
			ch <- err
		}
		close(ch)
	}()

	for {
		select {
		case <-ctx.Done(): //context dealine execeed!
			return ctx.Err()
		case err := <-ch:
			return err
		}
	}
}
func (wal *WriteAheadLog) closeCh(index int) {
	wal.mu.Lock()

	defer wal.mu.Unlock()

	delete(wal.commitCh, index)
}

func (wal *WriteAheadLog) RoleState() (int, bool) {
	return wal.rf.GetState()
}

func (wal *WriteAheadLog) ServeCommand(msg raft.ApplyMsg) {
	wal.mu.Lock()
	defer wal.mu.Unlock()
	var reply WaitResult
	var err error

	if msg.CommandValid {
		_, ok := msg.Command.(InvalidApplyMsg)
		if ok {
			wal.readOpinit = (msg.CommandTerm << 16) | 1&0xffff
			wal.readinitsem.Broadcast()
			wal.LastApplyIndex = msg.CommandIndex
			return
		}
	}

	op := msg.Command.(LogOp)
	index := msg.CommandIndex
	common.DPrintf("[S][INFO] Server %v ServeCommand,Index %v", wal.me, msg.CommandIndex)
	if resp, ok := wal.clientMap[op.ClientId]; ok && resp.Seq >= op.Seq && resp.SSeq >= op.SSeq && op.Region != "internal" {
		return
	}

	reply.Term = msg.CommandTerm
	err, reply.Err = wal.cmd.ServeApplyCommand(msg)
	if err != nil {
		return
	}
	if ch, ok := wal.commitCh[index]; ok {
		ch <- reply // 持锁？
	}
	common.DPrintf("[S][INFO] Update ClientSeqMap,New Seq %v", op.Seq)
	if op.Region != "internal" {
		wal.clientMap[op.ClientId] = SeqResponce{
			Seq:  op.Seq,
			SSeq: op.SSeq,
			Resp: reply,
		}
	}

	wal.LastApplyIndex = msg.CommandIndex

	if wal.shouldSnapShot() {
		wal.CreateSnapShot(wal.LastApplyIndex)
	}
}

func (wal *WriteAheadLog) startCommand(ClientId, Seq int64, cmd interface{}) error {
	wal.mu.Lock()
	op := cmd.(LogOp)
	if resp, ok := wal.clientMap[ClientId]; ok && resp.Seq >= Seq && resp.SSeq >= op.SSeq && op.Region != "internal" {
		log.Printf("duplicate request cid %v,seq %v", ClientId, Seq)
		wal.mu.Unlock()
		return types.ErrDuplicate
	}
	wal.mu.Unlock()

	index, term, isLeader := wal.rf.Start(op)
	if !isLeader {
		return types.ErrRedirect
	}
	commitCh := make(chan WaitResult, 1)
	wal.mu.Lock()
	wal.commitCh[index] = commitCh
	wal.mu.Unlock()
	defer func() {
		go wal.closeCh(index)
	}()
	select {
	case resp := <-commitCh:
		if resp.Term != term {
			return types.ErrRedirect
		} else {
			return resp.Err
		}
	case <-time.After(common.ProposalTimeout):
		return types.ErrTimeOut
	}
}

// 被动
func (wal *WriteAheadLog) ServeSnapShot(msg raft.ApplyMsg) {
	wal.mu.Lock()
	defer wal.mu.Unlock()
	common.DPrintf("[S][INFO] Server %v ServeSnapShot,SnapIndex %v", wal.me, msg.SnapshotIndex)
	if wal.LastApplyIndex >= msg.SnapshotIndex {
		common.DPrintf("[S][WARN] SnapShot Stale, My SnapIndex %v ,Apply SnapIndex %v", wal.LastApplyIndex, msg.SnapshotIndex)
		return
	}

	if wal.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
		wal.LastApplyIndex = msg.SnapshotIndex
		if msg.Snapshot == nil {
			return
		}
		wal.readSnapShot(msg.Snapshot)
	}
}
func (wal *WriteAheadLog) shouldSnapShot() bool {
	if wal.maxraftstate == -1 {
		return false
	}

	rate := float64(wal.rf.GetRaftStateSize()) / float64(wal.maxraftstate)

	return rate >= 0.95
}
func (wal *WriteAheadLog) readSnapShot(snap []byte) {
	r := bytes.NewBuffer(snap)
	d := gob.NewDecoder(r)
	if snap == nil || len(snap) < 1 { // bootstrap without any state?
		return
	}

	// var db map[string]string
	// var csm map[int64]ClientResponce
	var index int
	var csm map[int64]SeqResponce
	var bytes []byte
	if d.Decode(&index) != nil || d.Decode(&csm) != nil || d.Decode(&bytes) != nil {
		common.DPrintf("[S][FAIL] Server %v readSnapShot Decode error ", wal.me)
		return
	} else {
		wal.LastApplyIndex = index
		wal.clientMap = csm
		wal.cmd.InstallSnapShot(bytes)
	}
	common.DPrintf("[S][INFO] Server %v readSnapShot Success", wal.me)
}

func (wal *WriteAheadLog) NotifySnapShot() {
	wal.CreateSnapShot(wal.LastApplyIndex)
}

// 主动
func (wal *WriteAheadLog) CreateSnapShot(index int) {

	buf := new(bytes.Buffer)

	e := gob.NewEncoder(buf)
	bytes, err := wal.cmd.ServeSnapShot()
	if err != nil {
		panic(err)
	}
	if e.Encode(&wal.LastApplyIndex) != nil || e.Encode(&wal.clientMap) != nil || e.Encode(bytes) != nil {
		common.DPrintf("[S][FAIL] Server %v CreateSnapShot Encoding error ", wal.me)
		return
	}

	snap := buf.Bytes()
	common.DPrintf("[S][INFO] Server %v CreateSnapShot SnapIndex %v", wal.me, index)
	wal.rf.Snapshot(index, snap)
}

func (wal *WriteAheadLog) listen() {

	for !wal.stoped() {
		applyMsg := <-wal.applyCh
		if applyMsg.CommandValid {
			wal.ServeCommand(applyMsg)
		} else if applyMsg.SnapshotValid {
			wal.ServeSnapShot(applyMsg)
		} else {
			common.DPrintf("[S][FAIL] Server %v Apply Unsupported msg", wal.me)
		}
	}
}

func (wal *WriteAheadLog) Stop() {
	atomic.StoreInt32(&wal.dead, 1)
	wal.rf.Kill()
	// Your code here, if desired.
}

func (wal *WriteAheadLog) stoped() bool {
	z := atomic.LoadInt32(&wal.dead)
	return z == 1
}
func (wal *WriteAheadLog) SubscribeRoleChange(f func(l bool, who int)) {
	go func() {
		for {
			e := <-wal.rf.Event
			_, l := wal.RoleState()
			f(l, e)
		}
	}()
}

func (wal *WriteAheadLog) CheckLocalReadStat() bool {
	_, l := wal.RoleState()
	return l && wal.JudgeLocalRead()
}

func (wal *WriteAheadLog) JudgeLocalRead() bool {
	ctx := context.Background()

	err := wal.ReadIndexInvoker(ctx)

	if err != nil {
		common.LWarn(" %v", err)
		return false
	}

	return true
}

func (wal *WriteAheadLog) watchloop() {
	for event := range wal.rolechange {
		role := event.Arg.(int)
		if role != raft.Leader {
			continue
		}
		wal.readIndexInit(event.Term)
	}
}

func (wal *WriteAheadLog) followerReadIndexInvoker() error {
	return nil
}

func (wal *WriteAheadLog) ReadIndexInvoker(ctx context.Context) error {
	term, ok := wal.rf.GetState()

	if !ok {
		return wal.followerReadIndexInvoker()
	}

	wal.waitReadIndexInit(term)
	common.LInfo("readindex already ready")
	li := wal.rf.GetCommitIndex()
	wal.rsu.Lock()
	wal.readindex = li
	wal.rsu.Unlock()
	ctx, h := context.WithTimeout(ctx, common.LoaclReadTimeout)
	ch := wal.rf.SyncLease()
	defer h()
	select {
	case <-ctx.Done():
		return types.ErrTimeOut
	case <-ch:
	}

	common.LInfo("Readindex synclease over")

	//sl := SpinLock{}
	count := 1

	wal.mu.Lock()

	for wal.LastApplyIndex < li {
		wal.mu.Unlock()
		for i := 0; i < count; i++ {
			runtime.Gosched()
		}
		if count < 8 {
			count++
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		wal.mu.Lock()
	}
	wal.mu.Unlock()
	common.LInfo("Readindex >= Applyindex")
	return nil
}

type InvalidApplyMsg struct{}

// leader 调用
func (wal *WriteAheadLog) waitReadIndexInit(curTerm int) {

	wal.rsu.Lock()
	defer wal.rsu.Unlock()
	for wal.readOpinit>>16 != curTerm || wal.readOpinit&0xffff != 1 {
		wal.readinitsem.Wait()
	}
}

func (wal *WriteAheadLog) readIndexInit(term int) {
	wal.rsu.Lock()
	defer wal.rsu.Unlock()
	// 判断当前term内有没有初始化过readindex
	if (wal.readOpinit>>16) == term && wal.readOpinit&0xffff == 1 {
		return
	}
	//wal.readOpinit = (term << 16) | 1&0xffff

	initop := InvalidApplyMsg{}

	wal.rf.Start(initop)
}

func (wal *WriteAheadLog) Getrf() *raft.Raft {
	return wal.rf
}
func StartWalPeer(cmd ICommandLet, servers []*rpc.ClientEnd, me int, persiter types.IPersiter, maxraftstate int) *WriteAheadLog {
	wal := new(WriteAheadLog)
	wal.me = me
	wal.maxraftstate = maxraftstate
	wal.applyCh = make(chan raft.ApplyMsg, 1)
	wal.cmd = cmd
	wal.commitCh = make(map[int]chan WaitResult)
	wal.rf = raft.Make(servers, me, persiter, wal.applyCh)
	wal.clientMap = make(map[int64]SeqResponce)
	wal.rolechange = wal.rf.Watch(raft.ROLE_CHANGE)
	wal.readindex = 0
	wal.readinitsem = sync.NewCond(&wal.rsu)
	wal.readSnapShot(persiter.ReadSnapshot())
	go wal.listen()
	go wal.watchloop()
	return wal
}
