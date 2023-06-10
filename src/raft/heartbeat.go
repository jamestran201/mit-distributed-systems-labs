package raft

import (
	"fmt"
	"time"
)

func startHeartBeat(rf *Raft) {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go sendHeartBeatLoop(rf, i)
	}
}

func sendHeartBeatLoop(rf *Raft, server int) {
	var stopCh chan bool
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

		if stopCh != nil {
			stopCh <- true
		}

		stopCh = make(chan bool, 1)
		go sendHeartBeat(rf, server, stopCh)

		time.Sleep(300 * time.Millisecond)
	}
}

func sendHeartBeat(rf *Raft, server int, stopChan chan bool) {
	if rf.killed() {
		return
	}

	shouldExit := false
	args := &AppendEntriesArgs{}
	withLock(&rf.mu, func() {
		if rf.state != leader {
			shouldExit = true

			debugLog(rf, "Exiting sendHeartBeat routine because server is no longer the leader")
			return
		}

		args = &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndexForServer(rf, server),
			PrevLogTerm:  prevLogTermForServer(rf, server),
			Entries:      logEntriesToSend(rf, server),
			LeaderCommit: rf.commitIndex,
		}
	})

	if shouldExit {
		return
	}

	debugLog(rf, fmt.Sprintf("Sending heartbeat to %d", server))

	reply := &AppendEntriesReply{}
	sendAppendEntries(rf, server, args, reply)

	shouldExit = <-stopChan
	if shouldExit {
		return
	}

	handleAppendEntriesResponse(rf, reply)
}
