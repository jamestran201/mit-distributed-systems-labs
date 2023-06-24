package raft

import (
	"fmt"
	"log"
	"sync"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func withLock(mu *sync.Mutex, f func()) {
	mu.Lock()
	defer mu.Unlock()

	f()
}
func debugLogPlain(rf *Raft, msg string) {
	fmt.Printf("Server %d - %s\n", rf.me, msg)
}

func debugLog(rf *Raft, msg string) {
	fmt.Printf("Server %d - %s - %s\n", rf.me, msg, additionalLogInfo(rf))
}

func debugLogForRequestPlain(rf *Raft, traceId string, msg string) {
	fmt.Printf("Server %d - TraceId: %s - %s\n", rf.me, traceId, msg)
}

func debugLogForRequest(rf *Raft, traceId string, msg string) {
	fmt.Printf("Server %d - TraceId: %s - %s - %s\n", rf.me, traceId, msg, additionalLogInfo(rf))
}

func additionalLogInfo(rf *Raft) string {
	state := ""
	switch rf.state {
	case follower:
		state = "follower"
	case candidate:
		state = "candidate"
	case leader:
		state = "leader"
	}
	return fmt.Sprintf("Current term: %d, Voted for: %d, State: %s, Last log index: %d, Last log term: %d, Commit index: %d, Last applied: %d", rf.currentTerm, rf.votedFor, state, rf.logs.lastLogIndex, rf.logs.lastLogTerm, rf.commitIndex, rf.lastApplied)
}
