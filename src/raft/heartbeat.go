package raft

import (
	"time"
)

func startHeartBeat(rf *Raft) {
	for server := range rf.peers {
		if server == rf.me {
			continue
		}

		go sendHeartBeatLoop(rf, server)
	}
}

func sendHeartBeatLoop(rf *Raft, server int) {
	for {
		if rf.killed() {
			return
		}

		shouldExit := false
		withLock(&rf.mu, func() {
			shouldExit = rf.state != leader
		})

		if shouldExit {
			return
		}

		go callAppendEntries(rf, server, true)

		time.Sleep(300 * time.Millisecond)
	}
}
