package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"bytes"
	"encoding/gob"
	"gdfs/internal/common"
	"gdfs/internal/common/rpc"
	"gdfs/types"
	"io"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int
	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Entry struct {
	Term    int
	Index   int
	Command interface{}
}

// defination
var Leader = 1
var Follower = 0
var Candiate = 2
var MaxElecFailTimes = 5

const HeartBeat = 150

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex       // Lock to protect shared access to this peer's state
	peers     []*rpc.ClientEnd // RPC end points of all peers
	persister types.IPersiter  // Object to hold this peer's persisted state
	me        int              // this peer's index into peers[]
	dead      int32            // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	ApplyCond *sync.Cond
	ApplyCh   chan ApplyMsg
	// 5.1 5.3 持久化存储
	CurrentTerm       int
	VoteFor           int
	logEntries        []Entry
	LastSnapShotIndex int
	LastSnapShotTerm  int
	// for all
	CommitIndex      int // server always match the last log entry index
	LastAppliedIndex int
	State            int
	//
	NextIndex         []int
	MatchIndex        []int
	ElecFailTime      int
	HeartBeatDuration time.Duration
	ElecTimeout       ElecDealineTimer
	Lease             LeaderLease

	Event chan int

	EventMap map[EventType][]chan Event
}

func (rf *Raft) HasLeaderLease() bool {
	return len(rf.peers) == 1 || rf.Lease.OnLease()
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	term = rf.CurrentTerm
	isleader = (rf.State == Leader)
	// Your code here (2A).
	return term, isleader
}

func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	rf.getRaftState(w)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) getRaftState(w io.Writer) {
	e := gob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VoteFor)
	e.Encode(rf.logEntries)
	// For SnapShot
	e.Encode(rf.LastSnapShotIndex)
	e.Encode(rf.LastSnapShotTerm)
}

func (rf *Raft) readPersist(data []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)

	if err := d.Decode(&rf.CurrentTerm); err != nil {
		common.LWarn("<Raft> read Raft.CurrentTerm fail %v", err)
		return
	}
	if err := d.Decode(&rf.VoteFor); err != nil {
		common.LWarn("<Raft> read Raft.VoteFor fail %v", err)
		return
	}
	if err := d.Decode(&rf.logEntries); err != nil {
		common.LWarn("<Raft> read Raft.logEntries fail %v", err)
		return
	}
	if err := d.Decode(&rf.LastSnapShotIndex); err != nil {
		common.LWarn("<Raft> read Raft.LastSnapShotIndex fail %v", err)
		return
	}
	if err := d.Decode(&rf.LastSnapShotTerm); err != nil {
		common.LWarn("<Raft> read Raft.LastSnapShotTerm fail %v", err)
		return
	}
	rf.CommitIndex = rf.LastSnapShotIndex //
	rf.LastAppliedIndex = rf.CommitIndex
}

func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	return true
}

// 由上层服务主动调用
// @index 是全局索引定位
// 1 2 3
// 1 2 3 4 5
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.LastSnapShotIndex || index > rf.getLastLogIndex() {
		// log
		return
	}
	offset := index - rf.LastSnapShotIndex // index位置在log中真正的位置
	// 调整Raft状态
	rf.CommitIndex = max(rf.CommitIndex, index)
	rf.LastAppliedIndex = max(rf.LastAppliedIndex, index)
	rf.LastSnapShotTerm = rf.GlobalFetchLog(index).Term
	rf.LastSnapShotIndex = index
	rf.logEntries = rf.logEntries[offset:]
	w := new(bytes.Buffer)
	rf.getRaftState(w)
	rf.persister.SaveStateAndSnapshot(w.Bytes(), snapshot)

	//DPrintf("[R][%v] Server %v Service Call SnapShot On Index %v After Logs %v LastSnapShotIndex %v LastSnapShotTerm %v", GetRole(rf.State), rf.me, index, rf.logEntries, rf.LastSnapShotIndex, rf.LastSnapShotTerm)
	common.LInfo("<Raft> [%v] Server %v Service Call SnapShot On Index %v After Logs %v LastSnapShotIndex %v LastSnapShotTerm %v", GetRole(rf.State), rf.me, index, rf.logEntries, rf.LastSnapShotIndex, rf.LastSnapShotTerm)
}

type InstallSnapShotArg struct {
	Term             int
	LeaderId         int
	LastIncludeIndex int
	LastIncludeTerm  int
	SnapShot         []byte
}

type InstallSnapShotReply struct {
	Term int
}

func (rf *Raft) InstallSnapShot(args *InstallSnapShotArg, reply *InstallSnapShotReply) (err error) {
	rf.mu.Lock()
	//DPrintf("[R][%v] [INFO] Server %v InstallSnapShot From Leader %v", GetRole(rf.State), rf.me, args.LeaderId)
	common.LTrace("<Raft>[%v] Server %v InstallSnapShot From Leader %v", GetRole(rf.State), rf.me, args.LeaderId)
	reply.Term = rf.CurrentTerm
	if args.Term < rf.CurrentTerm {
		//DPrintf("[R][%v] [WARN] Server %v Reject InstallSnapShot From %v,My Term %v,Caller Term %v", GetRole(rf.State), rf.me, args.LeaderId, rf.CurrentTerm, args.Term)
		common.LWarn("<Raft>[%v] Server %v Reject InstallSnapShot From %v,My Term %v,Caller Term %v", GetRole(rf.State), rf.me, args.LeaderId, rf.CurrentTerm, args.Term)
		rf.mu.Unlock()
		return
	}
	if args.Term > rf.CurrentTerm {
		//rf.discoverNewTerm(args.Term) // 考虑是否需要改变votedFor为-1
		rf.CurrentTerm = args.Term
		rf.State = Follower
	}

	// refuse old snapshot
	if args.LastIncludeIndex < rf.LastSnapShotIndex {
		//DPrintf("[R][%v] [WARN] Server %v Reject InstallSnapShot From %v,My SnapShotIndex %v ,Caller SnapShotIndex %v", GetRole(rf.State), rf.me, args.LeaderId, rf.LastSnapShotIndex, args.LastIncludeIndex)
		common.LWarn("<Raft> Server %v Reject InstallSnapShot From %v,My SnapShotIndex %v ,Caller SnapShotIndex %v", GetRole(rf.State), rf.me, args.LeaderId, rf.LastSnapShotIndex, args.LastIncludeIndex)
		rf.mu.Unlock()
		return
	}

	offset := args.LastIncludeIndex - rf.LastSnapShotIndex

	// 有可能 args.LastIncludeIndex > len(rf.logEntries)
	if args.LastIncludeIndex > rf.getLastLogIndex() { // < len(rf.logEntries)
		rf.logEntries = []Entry{}
	} else {
		rf.logEntries = rf.logEntries[offset:]
	}
	rf.LastSnapShotTerm = args.LastIncludeTerm
	rf.LastSnapShotIndex = args.LastIncludeIndex
	rf.LastAppliedIndex = max(rf.LastAppliedIndex, rf.LastSnapShotIndex)
	rf.CommitIndex = max(rf.CommitIndex, rf.LastSnapShotIndex)
	w := new(bytes.Buffer)
	rf.getRaftState(w)
	state := rf.State
	//DPrintf("[R][%v] Server %v RefreshTimer", GetRole(rf.State), rf.me)
	rf.persister.SaveStateAndSnapshot(w.Bytes(), args.SnapShot)
	//rf.ElecTimeout.RefreshElecTimer()
	//DPrintf("[R][%v] [INFO] Server %v SnapShot Installed,SnapShotIndex %v Logs %v", GetRole(state), rf.me, rf.LastSnapShotIndex, rf.logEntries)
	common.LTrace("<Raft> [%v] Server %v SnapShot Installed,SnapShotIndex %v Logs %v", GetRole(state), rf.me, rf.LastSnapShotIndex, rf.logEntries)
	rf.mu.Unlock()

	rf.ApplyCh <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.SnapShot,
		SnapshotTerm:  args.LastIncludeTerm,
		SnapshotIndex: args.LastIncludeIndex, //全局索引
	}
	return
}

func (rf *Raft) LeaderSendSnapShot(server int, args *InstallSnapShotArg) {

	reply := InstallSnapShotReply{}
	if rf.sendInstallSnapShot(server, args, &reply) {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if reply.Term > rf.CurrentTerm {
			// rf.notify(server)
			rf.discoverNewTerm(reply.Term)
		}
		//rf.ElecTimeout.RefreshElecTimer()
		//rf.NextIndex[] = rf.LastSnapShotIndex + 1
		common.LTrace("<Raft>[%v] InstallSnapShot To %v", GetRole(rf.State), server)
	} else {
		//DPrintf("[R][Leader] [FAIL] SendSnapShot to Server %v,Network Fail", server)
		common.LWarn("<Raft> %v SendSnapShot to Server %v,Network Fail", GetRole(rf.State), server)
	}
}

func (rf *Raft) sendInstallSnapShot(server int, args *InstallSnapShotArg, reply *InstallSnapShotReply) bool {
	return rf.peers[server].Call("Raft.InstallSnapShot", args, reply) == nil
}

type PreVoteArgs struct {
	Term        int
	CandidateId int

	LastLogIndex int
	LastLogTerm  int
}

type PreVoteReply struct {
	Term    int
	Success bool
}

func (rf *Raft) sendPreVote(server int, args *PreVoteArgs, reply *PreVoteReply) bool {
	err := rf.peers[server].Call("Raft.PreVote", args, reply)

	if err != nil {
		common.LWarn("sendPreVote error %v", err)
	}
	return err == nil
}

func (rf *Raft) PreVote(args *PreVoteArgs, reply *PreVoteReply) (err error) {
	rf.mu.Lock()
	term := rf.CurrentTerm
	state := rf.State
	votedFor := rf.VoteFor
	lastLog := rf.getLastLog()
	rf.mu.Unlock()

	//DPrintf("[R][%v] [INFO] Server %v Get PreVote from %v args:%v", GetRole(state), rf.me, args.CandidateId, *args)
	common.LTrace("<Raft>%v Server %v Get PreVote from %v args:%v", GetRole(state), rf.me, args.CandidateId, *args)
	if args.Term > term {
		term = args.Term
		votedFor = -1
	}
	reply.Term = term
	up := false
	if args.LastLogTerm > lastLog.Term {
		up = true
	} else if args.LastLogTerm == lastLog.Term {
		up = args.LastLogIndex >= lastLog.Index
	}

	if votedFor == -1 && up {
		reply.Success = true
	}
	return
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandidateId int

	// 5.4 Safety
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term    int
	Success bool
}

// example RequestVote RPC handler.
// 可能面临以下情况
// 理论上只有第一个比本term大的才可能投票
// 1.正在选举中，收到其他投票请求
// 2.已经投票，但是又收到其他高term的投票
// 3.leader有可能收到投票请求
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) (err error) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.CurrentTerm {
		// rf.notify(args.CandidateId)
		rf.discoverNewTerm(args.Term)
	}

	upTodateFlag := rf.checkLogSafety(args)
	// candidate的票永远vote for self，这里可以直接判断，拒绝所有小于当前term投票请求
	if rf.VoteFor == -1 && upTodateFlag {
		rf.VoteFor = args.CandidateId
		reply.Success = true
		rf.persist()
		rf.ElecTimeout.RefreshElecTimer()
	} else {
		reply.Success = false
	}
	reply.Term = rf.CurrentTerm
	return
}

// 判断是否投票
// 对应论文中 5.4.1
// 必须check投票是否有效
func (rf *Raft) checkLogSafety(args *RequestVoteArgs) bool {

	// if args.LastLogIndex == 0 && len(rf.logEntries) == 0 || len(rf.logEntries) == 0 {
	// 	return true
	// }
	lastlog := rf.getLastLog()
	//DPrintf("[R][%v][INFO]<LOG-CHECK> Server %v logs:%v, requestVote Args:%v", GetRole(rf.State), rf.me, rf.logEntries, *args)
	common.LTrace("<Raft> Server %v logs:%v, requestVote Args:%v", GetRole(rf.State), rf.me, rf.logEntries, *args)
	if args.LastLogTerm > lastlog.Term {
		return true
	} else if args.LastLogTerm == lastlog.Term {
		return args.LastLogIndex >= lastlog.Index //日志一致就可以投票
	} else {
		return false
	}
}

// rpc请求中发现了更高的term号
// 更新到下一轮投票中
func (rf *Raft) discoverNewTerm(Term int) {
	//DPrintf("[R][%v][INFO] Server %v Convert to Follower,Origin Term %v,New Term %v", GetRole(rf.State), rf.me, rf.CurrentTerm, Term)
	common.LInfo("<Raft> Server %v Convert to Follower,Origin Term %v,New Term %v", GetRole(rf.State), rf.me, rf.CurrentTerm, Term)
	rf.CurrentTerm = Term
	rf.State = Follower
	rf.VoteFor = -1
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok != nil {
		common.LWarn("sendRequestVote error %v", ok)
	}
	return ok == nil
}
func (rf *Raft) Role() int {
	return rf.State
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.State != Leader {
		return -1, rf.CurrentTerm, false
	}

	index := rf.getLastLogIndex() + 1
	term := rf.CurrentTerm

	rf.logEntries = append(rf.logEntries, Entry{
		Command: command,
		Index:   index,
		Term:    term,
	})
	//DPrintf("[R][Leader][%v] [INFO] Receive Command,Index %v Command %v", rf.me, index, command)
	common.LTrace("<Raft>[Leader][%v] Receive Command,Index %v Command %v", rf.me, index, command)
	rf.persist()
	//也可以等待下一轮心跳在发送
	rf.doAE(false)

	// Your code here (2B).

	return index, term, true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

type AppendEntriesArgs struct {
	Term           int
	LeaderId       int
	PrevLogIndex   int //全局索引
	PrevLogTerm    int
	Entries        []Entry
	LeaderCommitId int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	// fast rollback support
	Conflict       bool //日志是否冲突
	HTerm          int  //冲突位置的term
	HLen           int  //日志长度
	HIndex         int  //第一次出现HTerm的index
	HSnapShotIndex int
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	if ok != nil {
		common.LWarn("sendAE error %v", ok)
	}
	return ok == nil
}

// 需要修改以适应2D snapshot
// 不可能出现Follower已持久的快照会比Leader更新
func (rf *Raft) AppendEntry(args *AppendEntriesArgs, reply *AppendEntriesReply) (err error) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false
	reply.Term = rf.CurrentTerm
	//DPrintf("[R][%v][INFO] Server %v,Receive AE %v", GetRole(rf.State), rf.me, GetAEReport(args))
	//DPrintf("[R]Report Self Log:%v", rf.logEntries)
	common.LTrace("<Raft>[%v] Server %v,Receive AE %v", GetRole(rf.State), rf.me, GetAEReport(args))
	if args.Term < rf.CurrentTerm {
		//DPrintf("[R][%v][WARN] Server %v,Reject AE From %v,My Term %v,AE Term:%v", GetRole(rf.State), rf.me, args.LeaderId, rf.CurrentTerm, args.Term)
		common.LWarn("<Raft>[%v] Server %v,Reject AE From %v,My Term %v,AE Term:%v", GetRole(rf.State), rf.me, args.LeaderId, rf.CurrentTerm, args.Term)
		return
	}

	//DPrintf("[R][%v] Server %v RefreshTimer", GetRole(rf.State), rf.me)
	rf.ElecTimeout.RefreshElecTimer()
	if args.Term > rf.CurrentTerm {
		rf.discoverNewTerm(args.Term)
		rf.notify(args.LeaderId)
		return
	}
	if rf.State == Candiate {
		rf.State = Follower
	}
	// 需要区别快照
	// 日志或快照落后
	// Leader通过判断HSnapShotIndex快照是否落后
	if rf.getLastLogIndex() < args.PrevLogIndex {
		reply.Conflict = true
		reply.HTerm = -1
		reply.HLen = len(rf.logEntries)
		reply.HIndex = -1
		reply.HSnapShotIndex = rf.LastSnapShotIndex //
		return
	}

	if args.PrevLogIndex <= 0 && len(args.Entries) == 0 {
		reply.Success = true
		reply.Conflict = false                      //
		reply.HSnapShotIndex = rf.LastSnapShotIndex //
		return
	}
	// 快照一致并且日志冲突
	if rf.getLastLogIndex() >= args.PrevLogIndex && rf.GlobalFetchLog(args.PrevLogIndex).Term != args.PrevLogTerm {
		//DPrintf("[R][Follower][WARN] Log didn't match,myLog:%v,SnapShotIndex %v,SnapShotTerm %v", rf.logEntries, rf.LastSnapShotIndex, rf.LastSnapShotTerm)
		common.LWarn("<Raft>[Follower] Log didn't match,myLog:%v,SnapShotIndex %v,SnapShotTerm %v", rf.logEntries, rf.LastSnapShotIndex, rf.LastSnapShotTerm)
		reply.Conflict = true
		hterm := rf.GlobalFetchLog(args.PrevLogIndex).Term

		for index := args.PrevLogIndex; index > rf.LastSnapShotIndex; index-- {
			if rf.GlobalFetchLog(index-1).Term != hterm {
				reply.HIndex = index
				break
			}
		}
		reply.HLen = len(rf.logEntries)
		return
	}

	match := args.PrevLogIndex
	offset := match - rf.LastSnapShotIndex
	if offset <= 0 {
		rf.logEntries = []Entry{}
		//rf.logEntries = append(rf.logEntries, args.Entries...)
		if len(args.Entries) > -offset {
			rf.logEntries = append(rf.logEntries, args.Entries[-offset:]...)
		}
	} else {
		rf.logEntries = append(rf.logEntries[:offset], args.Entries...)
	}
	rf.persist()
	// 截断所有不匹配位置之后的日志

	if args.LeaderCommitId > rf.CommitIndex {
		rf.CommitIndex = min(args.LeaderCommitId, rf.getLastLogIndex())
		rf.Apply()
	}
	DPrintf("[R][%v][INFO] Report Self Log:%v", GetRole(rf.State), rf.logEntries)
	common.LTrace("<Raft>[%v] Report Self Log:%v", GetRole(rf.State), rf.logEntries)
	reply.Success = true
	return
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a > b {
		return b
	}
	return a
}
func (rf *Raft) LeaderSendAE(peer int, args *AppendEntriesArgs, done func(bool, int)) {
	var resp AppendEntriesReply
	// if rf.State != Leader {
	// 	return
	// }
	ok := rf.sendAppendEntry(peer, args, &resp)

	if !ok {
		common.LWarn("<Raft>[Leader] Server:%v SendAE to %v FAIL", rf.me, peer)
		done(false, peer)
		return //遭遇网络故障
	}
	done(true, peer)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if resp.Term > rf.CurrentTerm {
		// rf.notify(peer)
		rf.discoverNewTerm(resp.Term)
		return
	}
	//DPrintf("[R]Server:%v, AE-Reply: %v", peer, resp)
	if args.Term == rf.CurrentTerm {
		if resp.Success {
			match := args.PrevLogIndex + len(args.Entries)
			next := match + 1
			rf.NextIndex[peer] = max(rf.NextIndex[peer], next)
			rf.MatchIndex[peer] = max(rf.MatchIndex[peer], match)
			common.LTrace("<Raft>[Leader][INFO] Server:%v,NextIndex:%v,MatchIndex:%v", peer, rf.NextIndex[peer], rf.MatchIndex[peer])
			//DPrintf("[R][Leader][INFO] Server:%v,NextIndex:%v,MatchIndex:%v", peer, rf.NextIndex[peer], rf.MatchIndex[peer])
		} else if resp.Conflict {
			common.LTrace("<Raft>[%v]<AE> Server %v -> Server %v RollBack [%v]", GetRole(rf.State), rf.me, peer, resp)
			//DPrintf("[R][%v][INFO]<AE> Server %v -> Server %v RollBack [%v]", GetRole(rf.State), rf.me, peer, resp)
			if resp.HTerm == -1 {
				// follower 日志落后，直接调整到日志长度位置
				// 判断是否缺少日志
				if resp.HSnapShotIndex < rf.LastSnapShotIndex {
					DPrintf("[R][%v][WARN] Server %v Report Lack of SnapShot,Reply %v", GetRole(rf.State), peer, resp)
					args := &InstallSnapShotArg{
						Term:             rf.CurrentTerm,
						LeaderId:         rf.me,
						LastIncludeIndex: rf.LastSnapShotIndex,
						LastIncludeTerm:  rf.LastSnapShotTerm,
						SnapShot:         rf.persister.ReadSnapshot(),
					}
					rf.NextIndex[peer] = rf.LastSnapShotIndex + 1
					common.LTrace("<Raft>[%v][WARN] InstallSnapShot -> Server %v SnapShotIndex %v SnapShotTerm %v", GetRole(rf.State), rf.me, rf.LastSnapShotIndex, rf.LastSnapShotTerm)
					//DPrintf("[R][%v][WARN] InstallSnapShot -> Server %v SnapShotIndex %v SnapShotTerm %v", GetRole(rf.State), rf.me, rf.LastSnapShotIndex, rf.LastSnapShotTerm)
					go rf.LeaderSendSnapShot(peer, args)
				} else {
					// 更新全局索引
					rf.NextIndex[peer] = resp.HLen + rf.LastSnapShotIndex
				}
			} else {
				//查找 follower冲突处的日志任期的最后一个日志
				//DPrintf("[R][%v][WARN] Server %v Miss Log", GetRole(rf.State), peer)
				common.LTrace("<Raft>[%v][WARN] Server %v Miss Log", GetRole(rf.State), peer)
				index := rf.FindLastMatchIndex(resp.HTerm)
				if index > 0 {
					//如果找到，准备添加日志
					rf.NextIndex[peer] = index
				} else {
					//如果没找到，准备覆盖这个不一致日志项
					rf.NextIndex[peer] = resp.HIndex
				}
			}
		} else if rf.NextIndex[peer] > 1 {
			rf.NextIndex[peer]-- //最低为1
		}
		rf.LeaderCommit()
	}
}

func (rf *Raft) FindLastMatchIndex(term int) int {
	for index := rf.getLastLogIndex(); index > rf.LastSnapShotIndex; index-- {
		if rf.GlobalFetchLog(index).Term == term {
			return index
		}
	}
	return 0
}

// index是相对日志首项的偏移
// index 已1开始，为0表示获取上一次快照的信息
func (rf *Raft) FetchLog(index int) Entry {
	if index >= 1 {
		return rf.logEntries[index-1]
	}
	return Entry{
		Index: rf.LastSnapShotIndex,
		Term:  rf.LastSnapShotTerm,
	}
}

// index是全局日志索引
// index不可以小于上一次快照的全局索引
func (rf *Raft) GlobalFetchLog(index int) Entry {
	offset := index - rf.LastSnapShotIndex
	if offset >= 1 {
		return rf.logEntries[offset-1]
	} else {
		return Entry{
			Command: nil,
			Term:    rf.LastSnapShotTerm,
			Index:   rf.LastSnapShotIndex,
		}
	}
}

// 这里有问题，如果一个新的leader再任期间没有提交日志，那么它的commitindex将不会变化！
// 这对实现local-lease-read有很大障碍，因为lease-read没有经过提案
// 考虑readindex的实现方案

func (rf *Raft) LeaderCommit() {
	if rf.State != Leader {
		return
	}

	// FIXME if all the log are smaller than

	for n := rf.CommitIndex + 1; n <= rf.getLastLogIndex(); n++ {
		// if rf.logEntries[n-1].Term != rf.CurrentTerm {
		// 	continue //过往日志直接跳过，只要当上leader就能保证日志在集群的大多数成员都存在，
		// 	// leader只对本任期的日志进行match匹配提交，过往的日志都是默认已提交的
		// }
		// rf.GlobalFetchLog(n-1).Term
		if rf.GlobalFetchLog(n).Term != rf.CurrentTerm {
			continue
		}
		counter := 1

		for server := range rf.peers { //对match的位置进行计数，如果大多数agree，才会提交
			if server != rf.me && rf.MatchIndex[server] >= n {
				counter++
			}

			if 2*counter > len(rf.peers) {
				rf.CommitIndex = n
				// time to apply
				common.LTrace("[R][%v] CommitIndex %v", GetRole(rf.State), rf.CommitIndex)
				//DPrintf("[R][%v] CommitIndex %v", GetRole(rf.State), rf.CommitIndex)
				rf.Apply()
				break
			}
		}
	}

}

// check 标识是否是心跳包
// 调用方必须持有锁
func (rf *Raft) doAE(check bool, done ...func()) {
	var count int32 = 1 //发往自身的AE永远成功
	aedone := func(stat bool, peer int) {
		if !stat {
			return
		}
		if len(rf.peers) > 1 {
			atomic.AddInt32(&count, 1)
		}
		if atomic.CompareAndSwapInt32(&count, int32(len(rf.peers)/2+1), 0) {
			common.LInfo("<Raft>[Leader] Keeping Lease")
			rf.Lease.keepLease(rf.ElecTimeout.TimeOuts)
			for _, v := range done {
				v()
			}
		}
	}
	if len(rf.peers) == 1 {
		//rf.ElecTimeout.RefreshElecTimer()
		rf.LeaderCommit()
		aedone(true, 0)
	}
	rf.Lease.startpoint() //记录lease起始时间
	for server := range rf.peers {
		if server == rf.me {
			rf.ElecTimeout.RefreshElecTimer()
			continue
		}
		if check {
			//DPrintf("[R][%v] Server %v AE-CHECK", GetRole(rf.State), rf.me)
			common.LTrace("<Raft>[%v] Server %v AE-CHECK", GetRole(rf.State), rf.me)
		}
		// 如果不是心跳包必须
		if check || rf.getLastLogIndex() >= rf.NextIndex[server] {
			index := rf.NextIndex[server]
			if index <= 0 {
				index = 1 //nextIndex 最低为0
			}
			if index > rf.getLastLogIndex()+1 {
				index = rf.getLastLogIndex()
			}
			prev := rf.GlobalFetchLog(index - 1)
			logs := make([]Entry, rf.getLastLogIndex()-index+1)
			offset := index - rf.LastSnapShotIndex
			if offset <= 0 {
				logs = []Entry{}
			} else {
				copy(logs, rf.logEntries[offset-1:])
			}
			req := &AppendEntriesArgs{
				Term:           rf.CurrentTerm,
				LeaderId:       rf.me,
				PrevLogIndex:   prev.Index,
				PrevLogTerm:    prev.Term,
				Entries:        logs,
				LeaderCommitId: rf.CommitIndex,
			}

			go rf.LeaderSendAE(server, req, aedone)
		}
	}
}

// Entry里面的Index都是全局索引
func (rf *Raft) getLastLogIndex() int {
	if len(rf.logEntries) == 0 {
		return rf.LastSnapShotIndex
	} else {
		return rf.logEntries[len(rf.logEntries)-1].Index
	}
}

// func (rf *Raft) getLastLogOffset() int {
// 	return len(rf.logEntries)
// }

func (rf *Raft) getLastLog() Entry {
	if len(rf.logEntries) == 0 {
		return Entry{
			Command: nil,
			Index:   rf.LastSnapShotIndex,
			Term:    rf.LastSnapShotTerm,
		}
	}
	return rf.logEntries[len(rf.logEntries)-1]
}

func (rf *Raft) getRequestVoteArg() RequestVoteArgs {
	if rf.getLastLogIndex() == 0 {
		return RequestVoteArgs{
			Term:         rf.CurrentTerm,
			CandidateId:  rf.me,
			LastLogIndex: 0,
			LastLogTerm:  0,
		}
	} else {
		log := rf.getLastLog()
		return RequestVoteArgs{
			Term:         rf.CurrentTerm,
			CandidateId:  rf.me,
			LastLogIndex: log.Index,
			LastLogTerm:  log.Term,
		}
	}
}

func (rf *Raft) getPreVoteArg() PreVoteArgs {
	if rf.getLastLogIndex() == 0 {
		return PreVoteArgs{
			Term:         rf.CurrentTerm + 1,
			CandidateId:  rf.me,
			LastLogIndex: 0,
			LastLogTerm:  0,
		}
	} else {
		log := rf.getLastLog()
		return PreVoteArgs{
			Term:         rf.CurrentTerm + 1,
			CandidateId:  rf.me,
			LastLogIndex: log.Index,
			LastLogTerm:  log.Term,
		}
	}
}

// 这个函数有和RequestVote有部分冲突的可能
// 1. RequestVote先把票
func (rf *Raft) leaderElection() {
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	rf.CurrentTerm++
	rf.VoteFor = rf.me
	//DPrintf("[R][%v][INFO] Leader-Elec:[%v],Term:[%v]", GetRole(rf.State), rf.me, rf.CurrentTerm)
	common.LInfo("<Raft>[%v]-[%v] election begin!,term %v", GetRole(rf.State), rf.me, rf.CurrentTerm)
	rf.persist()
	rf.State = Candiate
	args := rf.getRequestVoteArg()
	counter := 1
	for id := range rf.peers {
		if id != rf.me {
			go func(id int) {
				rf.CallRequestVote(id, &counter, &args)
			}(id)
		}
	}
}

func (rf *Raft) Campagin() {
	args := rf.getPreVoteArg()
	mux := &sync.Mutex{}
	camCount := 1
	for server := range rf.peers {
		if server == rf.me {
			//rf.ElecTimeout.RefreshElecTimer()
			continue
		}
		go func(peer int) {
			rf.CallPreVote(peer, &camCount, mux, &args)
		}(server)
	}
}

func (rf *Raft) CallPreVote(server int, counter *int, mux *sync.Mutex, args *PreVoteArgs) {
	resp := PreVoteReply{}
	if rf.sendPreVote(server, args, &resp) {
		// rf.mu.Lock()
		// defer rf.mu.Unlock()
		//DPrintf("[R][Campaign][DEBUG] Server %v Receive PreVoteReply %v", rf.me, resp)
		common.LTrace("<Raft>[Campaign] Server %v Receive PreVoteReply %v", rf.me, resp)
		mux.Lock()
		defer mux.Unlock()
		if resp.Success {
			*counter++
			//DPrintf("[R][Campaign][INFO] Server: %v Gain Vote form : %v > [%v/%v]", rf.me, server, *counter, len(rf.peers))
			common.LTrace("<Raft>[Campaign]Server: %v Gain Vote form : %v > [%v/%v]", rf.me, server, *counter, len(rf.peers))
		}
		if *counter*2 > len(rf.peers) {
			rf.mu.Lock()
			if rf.State != Leader {
				DPrintf("[R][Campaign][INFO] Server %v Ready to Election", rf.me)
				rf.leaderElection()
				*counter = 0 //只会举行一次选举
			}
			rf.mu.Unlock()
		}
	} else {
		common.LWarn("<Raft>[Campaign][FAIL] Call Server %v For PreVote,NetWork Fail", server)
	}
}

func (rf *Raft) CallRequestVote(server int, counter *int, args *RequestVoteArgs) {
	argv := RequestVoteReply{}
	if rf.sendRequestVote(server, args, &argv) {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		common.LWarn("<Raft>[%v] Server %v,Recevie RequestVoteReply: %v", GetRole(rf.State), rf.me, argv)
		if argv.Term > rf.CurrentTerm {
			//common.LInfo("[R][%v] Server: %v dicover higher Term: %v", GetRole(rf.State), rf.me, argv.Term)
			// rf.notify(server)
			rf.discoverNewTerm(argv.Term)
			return
		}
		if argv.Success {
			*counter++
			common.LInfo("<Raft>[%v] Server: %v Gain Vote form : %v > [%v/%v]", GetRole(rf.State), rf.me, server, *counter, len(rf.peers))
		}
		if rf.State == Candiate && *counter*2 > len(rf.peers) {
			rf.State = Leader
			rf.becomeLeader()
			rf.doAE(true)
		}
	} else {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		common.LWarn("<Raft>[%v] Server: %v,CallRequestVote Network FAIL", GetRole(rf.State), rf.me)
	}
}

// 调整自己成Leader
func (rf *Raft) becomeLeader() {
	common.LInfo("<Raft>[%v] become Leader,id:%v,Term:%v,logs:%v", GetRole(rf.State), rf.me, rf.CurrentTerm, rf.logEntries)
	rf.State = Leader
	rf.notify(rf.me)
	for i := 0; i < len(rf.peers); i++ {
		rf.NextIndex[i] = rf.getLastLogIndex() + 1
		rf.MatchIndex[i] = rf.CommitIndex
	}
	rf.ElecFailTime = 0

	// readindex implements
	//rf.readIndexInit()
	//DPrintf("[R][%v][DEBUG] NextIndex: %v", GetRole(rf.State), rf.NextIndex)
	//common.LInfo("<Raft>[%v] become Leader,id:%v,Term:%v,logs:%v", GetRole(rf.State), rf.me, rf.CurrentTerm, rf.logEntries)

	event := Event{
		Stat: ROLE_CHANGE,
		Arg:  Leader,
		Term: rf.CurrentTerm,
	}

	chs, ok := rf.EventMap[ROLE_CHANGE]

	if !ok {
		return
	}

	for _, v := range chs {
		select {
		case v <- event:
		default:
		}
	}
}

// 通知提交日志

func (rf *Raft) Apply() {
	rf.ApplyCond.Broadcast()
}

func (rf *Raft) Applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for !rf.killed() {
		if rf.CommitIndex > rf.LastAppliedIndex && rf.getLastLogIndex() > rf.LastAppliedIndex {
			rf.LastAppliedIndex++
			common.LInfo("<Raft>[%v][INFO] Server %v Apply,CommitIndex:%v,ApplyIndex:%v", GetRole(rf.State), rf.me, rf.CommitIndex, rf.LastAppliedIndex)
			//DPrintf("[R][%v][INFO] Server %v Apply,CommitIndex:%v,ApplyIndex:%v", GetRole(rf.State), rf.me, rf.CommitIndex, rf.LastAppliedIndex)
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.GlobalFetchLog(rf.LastAppliedIndex).Command,
				CommandIndex: rf.LastAppliedIndex,
				CommandTerm:  rf.CurrentTerm,
			}
			rf.mu.Unlock()
			rf.ApplyCh <- applyMsg
			rf.mu.Lock()
		} else {
			rf.ApplyCond.Wait()
		}
	}
}

// Raft时钟事件生成器

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	rf.InitRandomSleep(1)
	for !rf.killed() {
		// time.Sleep().
		time.Sleep(rf.HeartBeatDuration)
		rf.mu.Lock()
		switch rf.State {
		case Leader:
			rf.doAE(true)
		default:
			if rf.ElecTimeout.Check() {
				common.LInfo("<Raft>[%v][%v] election timeout!!,ready to campaign", GetRole(rf.State), rf.me)
				if len(rf.peers) == 1 {
					//rf.notify(rf.me)
					common.LInfo("<Raft>[%v] single node cluster,free of prevote to become leader", rf.me)
					rf.becomeLeader()
					break
				}
				rf.Campagin()
			}
		}
		rf.mu.Unlock()
	}
}

// Raft的选举时钟管理

type ElecDealineTimer struct {
	Deadline time.Time
	TimeOuts time.Duration
}

// 随机隔离选举超时，防止预选举分票
func (rf *Raft) InitRandomSleep(plus int) {
	n := rand.Intn(plus * int(rf.HeartBeatDuration) / int(time.Millisecond))
	n += 20
	common.LInfo("<Raft> init random sleep time", n)
	//DPrintf("[R][INFO] Init Sleep %v ms", n)
	time.Sleep(time.Duration(n * int(time.Millisecond)))
}

// 重新设置选举超时时间
func (ed *ElecDealineTimer) ResetTimout(td time.Duration) {
	ed.TimeOuts = td
	ed.Deadline = time.Now()
	ed.RefreshElecTimer()
}

// 刷新选举时钟
func (ed *ElecDealineTimer) RefreshElecTimer() {
	now := time.Now()
	dur := ed.Deadline.Sub(now)
	if dur <= 4*HeartBeat*time.Millisecond {
		ed.Deadline = ed.Deadline.Add(ed.TimeOuts)
	}
	//common.LTrace("<Raft> new exipre time in %v", ed.Deadline)
}

// 判断选举时钟是否过期
func (ed *ElecDealineTimer) Check() bool {
	return time.Now().After(ed.Deadline)
}

func (rf *Raft) GetRaftStateSize() int {
	return rf.persister.RaftStateSize()
}

func (rf *Raft) GetCommitIndex() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.CommitIndex
}

// 等待一轮lease同步完成，有可能永远同步不完成
func (rf *Raft) SyncLease() <-chan struct{} {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	ch := make(chan struct{}, 1)

	done := func() {
		select {
		case ch <- struct{}{}:
		default:
		}
	}

	rf.doAE(true, done)

	return ch
}

func (rf *Raft) notify(who int) {
	select {
	case rf.Event <- who:
	default:
	}
}

func (rf *Raft) RoleChange() <-chan int {
	return rf.Event
}

type EventType uint8

const (
	ROLE_CHANGE EventType = iota + 1
)

type Event struct {
	Stat EventType
	Arg  interface{}
	Term int
}

func (rf *Raft) Watch(t EventType) <-chan Event {

	if rf.EventMap == nil {
		rf.EventMap = make(map[EventType][]chan Event)
	}

	if _, ok := rf.EventMap[t]; !ok {
		rf.EventMap[t] = make([]chan Event, 0)
	}
	ch := make(chan Event)
	rf.EventMap[t] = append(rf.EventMap[t], ch)

	return ch
}

// 初始化一个Raft Peer

// peers 集群地址
// me 当前Server地址
// persister 持久化
// applyCh 提交通道
func Make(peers []*rpc.ClientEnd, me int,
	persister types.IPersiter, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	// Your initialization code here (2A, 2B, 2C).
	// 2A
	rf.State = Follower
	rf.VoteFor = -1
	rf.HeartBeatDuration = time.Millisecond * HeartBeat
	rf.NextIndex = make([]int, len(peers))
	rf.MatchIndex = make([]int, len(peers))
	rf.ApplyCh = applyCh
	rf.ApplyCond = sync.NewCond(&rf.mu)
	rf.CommitIndex = 0
	rf.LastAppliedIndex = 0
	rf.Lease = LeaderLease{}
	rf.Event = make(chan int)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	if rf.VoteFor != 0 && rf.VoteFor != -1 {
		//DPrintf("[R][Recover] Server %v,Term %v Log:%v", rf.me, rf.CurrentTerm, rf.logEntries)
		common.LInfo("<Raft> recover server %v, term %v log %v", rf.me, rf.CurrentTerm, rf.logEntries)
	}
	rf.ElecTimeout.ResetTimout(rf.HeartBeatDuration + rf.HeartBeatDuration/2)
	//DPrintf("[R]Make peer: %v, elec timeout: %v", rf.me, rf.ElecTimeout.TimeOuts)
	common.LInfo("<Raft> make peer %v, init election timeout expire %v", rf.me, rf.ElecTimeout.Deadline)
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.Applier()
	return rf
}
