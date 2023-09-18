package raft

import (
	"fmt"
	"log"
)

// Debugging
const Debug = false

func init() {
	log.SetFlags(log.Lmicroseconds)
}
func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func GetRole(role int) string {
	switch role {
	case 1:
		return "Leader"
	case 0:
		return "Follower"
	case 2:
		return "Candidate"
	}
	return "null"
}

func GetAEReport(args *AppendEntriesArgs) string {
	return fmt.Sprintf("[Term:%v,Server:%v,PrevLogIndex:%v,PrevLogTerm:%v,Logs:%v,CommitIndex:%v]", args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.Entries, args.LeaderCommitId)
}
