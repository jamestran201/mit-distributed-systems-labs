package raft

import (
	"crypto/rand"
	"encoding/base64"
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
	if !Debug {
		return
	}

	fmt.Printf("Server %d - %s\n\n", rf.me, msg)
}

func debugLog(rf *Raft, msg string) {
	if !Debug {
		return
	}

	fmt.Printf("Server %d - %s - %s\n\n", rf.me, msg, additionalLogInfo(rf))
}

func debugLogForRequestPlain(rf *Raft, traceId string, msg string) {
	if !Debug {
		return
	}

	fmt.Printf("Server %d - TraceId: %s - %s\n\n", rf.me, traceId, msg)
}

func debugLogForRequest(rf *Raft, traceId string, msg string) {
	if !Debug {
		return
	}

	fmt.Printf("Server %d - TraceId: %s - %s - %s\n\n", rf.me, traceId, msg, additionalLogInfo(rf))
}

func additionalLogInfo(rf *Raft) string {
	return fmt.Sprintf("Current term: %d, Voted for: %d, State: %s", rf.currentTerm, rf.votedFor, rf.state)
	// return fmt.Sprintf("Current term: %d, Voted for: %d, State: %s, Last log index: %d, Last log term: %d, Commit index: %d, Last applied: %d", rf.currentTerm, rf.votedFor, state, rf.logs.lastLogIndex, rf.logs.lastLogTerm, rf.commitIndex, rf.lastApplied)
}

func generateTraceId() string {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	if err != nil {
		panic(err)
	}
	return base64.URLEncoding.EncodeToString(b)
}
