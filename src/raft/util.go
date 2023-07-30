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

// const Debug = true

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
	return fmt.Sprintf("CurrentTerm: %d, VotedFor: %d, State: %s, LastLogIndex: %d, LastLogTerm: %d, CommitIndex: %d, LastApplied: %d", rf.currentTerm, rf.votedFor, rf.state, rf.lastLogIndex, rf.lastLogTerm, rf.commitIndex, rf.lastApplied)
}

func generateTraceId() string {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	if err != nil {
		panic(err)
	}
	return base64.URLEncoding.EncodeToString(b)
}
