package raft

import "fmt"

func considerStartingElection(rf *Raft) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state == leader {
		debugLog(rf, "Already leader, skipping election")
		return
	}

	switch rf.state {
	case follower:
		if !rf.receivedRpcFromPeer {
			debugLog(rf, "Did not receive RPC from peer, starting election")

			startElection(rf)
		} else {
			debugLog(rf, "Received RPC from peer, skipping election")
		}

		rf.receivedRpcFromPeer = false
	case candidate:
		debugLog(rf, "Candidate did not win election within the election timeout period, starting new election")

		startElection(rf)
	}
}

func startElection(rf *Raft) {
	rf.currentTerm++
	rf.state = candidate
	rf.votedFor = rf.me
	rf.votesReceived = 1
	rf.requestVotesResponsesReceived = 0

	rf.persist(nil)

	go requestVotesFromPeers(rf, rf.currentTerm)
}

func onLeaderElection(rf *Raft) {
	rf.state = leader

	nextIndex := make([]int, len(rf.peers))
	for i := range nextIndex {
		nextIndex[i] = rf.logs.lastLogIndex + 1
	}

	rf.nextIndex = nextIndex
	rf.matchIndex = make([]int, len(rf.peers))

	startHeartBeat(rf)

	debugLog(rf, fmt.Sprintf("Promoted to leader. Current term: %d", rf.currentTerm))
}
