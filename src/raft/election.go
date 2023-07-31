package raft

import (
	"fmt"
	"math/rand"
	"time"
)

func (rf *Raft) electionTimer() {
	for !rf.killed() {
		// pause for a random amount of time between 600 and 1600
		// milliseconds.
		ms := 600 + (rand.Int63() % 300)
		debugLogPlain(rf, fmt.Sprintf("Current election timeout is %d milliseconds", ms))

		time.Sleep(time.Duration(ms) * time.Millisecond)
		debugLogPlain(rf, "Election timeout period elapsed")

		rf.considerStartingElection()
	}
}

func (rf *Raft) considerStartingElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state == LEADER {
		debugLog(rf, "Already is leader, skipping election")
		return
	}

	switch rf.state {
	case FOLLOWER:
		if !rf.receivedRpcFromPeer {
			debugLog(rf, "Did not receive RPC from peer, starting election")

			rf.startElection()
		} else {
			debugLog(rf, "Received RPC from peer, skipping election")
		}

		rf.receivedRpcFromPeer = false
	case CANDIDATE:
		debugLog(rf, "Candidate did not win election within the election timeout period, starting new election")

		rf.startElection()
	}
}

func (rf *Raft) startElection() {
	rf.currentTerm++
	rf.state = CANDIDATE
	rf.votedFor = rf.me
	rf.votesReceived = 1
	rf.requestVotesResponsesReceived = 0

	rf.persist()

	go rf.requestVotesFromPeers(rf.currentTerm)
}

func (rf *Raft) requestVotesFromPeers(term int) {
	debugLog(rf, "Requesting votes")

	for i := 0; i < len(rf.peers); i++ {
		if rf.killed() {
			return
		}

		if i == rf.me {
			continue
		}

		go rf.callRequestVotes(term, i)
	}
}

func (rf *Raft) onLeaderElection() {
	rf.state = LEADER

	nextIndex := make([]int, len(rf.peers))
	for i := range nextIndex {
		nextIndex[i] = rf.lastLogIndex + 1
	}

	rf.nextIndex = nextIndex
	rf.matchIndex = make([]int, len(rf.peers))

	rf.startHeartBeat()

	debugLog(rf, fmt.Sprintf("Promoted to leader. Current term: %d", rf.currentTerm))
}
