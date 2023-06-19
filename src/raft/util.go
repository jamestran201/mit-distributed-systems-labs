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

func debugLog(rf *Raft, msg string) {
	fmt.Printf("Server %d - %s\n", rf.me, msg)
}

func debugLogForRequest(rf *Raft, traceId string, msg string) {
	fmt.Printf("Server %d - %s - TraceId: %s\n", rf.me, msg, traceId)
}
