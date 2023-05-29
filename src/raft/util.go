package raft

import (
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
