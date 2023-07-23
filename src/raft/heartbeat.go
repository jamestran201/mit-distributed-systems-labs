package raft

import "time"

func (rf *Raft) startHeartBeat() {
	for server := range rf.peers {
		if server == rf.me {
			continue
		}

		go rf.sendHeartBeatLoop(server)
	}
}

func (rf *Raft) sendHeartBeatLoop(server int) {
	for {
		if rf.killed() {
			return
		}

		shouldExit := false
		withLock(&rf.mu, func() {
			shouldExit = rf.state != LEADER
		})

		if shouldExit {
			return
		}

		go rf.callAppendEntries(server)

		time.Sleep(300 * time.Millisecond)
	}
}
