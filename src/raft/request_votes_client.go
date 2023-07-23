package raft

import "fmt"

func (rf *Raft) callRequestVotes(term, server int) {
	var args *RequestVoteArgs

	shouldExit := false
	withLock(&rf.mu, func() {
		if rf.killed() {
			debugLog(rf, "Sever is killed, aborting callRequestVotes routine.")

			shouldExit = true
			return
		}

		if rf.currentTerm != term || rf.state != CANDIDATE {
			debugLog(rf, fmt.Sprintf(
				"Candidate state changed, aborting requestVotes routine.\nCurrent term: %d, given term: %d.\nCurrent state: %s", rf.currentTerm, term, rf.state,
			))
			shouldExit = true
			return
		}

		args = rf.makeRequestVotesArgs()
	})

	if shouldExit {
		return
	}

	reply := &RequestVoteReply{}
	ok := rf.sendRequestVote(server, args, reply)
	if !ok {
		debugLogForRequestPlain(rf, args.TraceId, fmt.Sprintf("A RequestVote request to %d could not be processed successfully, skipping", server))
		return
	}

	rf.handleRequestVotesResponse(server, term, args, reply)
}

func (rf *Raft) makeRequestVotesArgs() *RequestVoteArgs {
	args := &RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
		TraceId:     generateTraceId(),
	}

	return args
}

// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	debugLogPlain(rf, fmt.Sprintf("Sending RequestVote to %d", server))

	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) handleRequestVotesResponse(server int, term int, args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	debugLogForRequest(rf, args.TraceId, fmt.Sprintf("Received RequestVote response from %d", server))

	if rf.killed() || rf.state != CANDIDATE || rf.currentTerm != term {
		debugLogForRequest(rf, args.TraceId, "State is outdated, exiting handleRequestVotesResponse routine")
		return
	}

	if reply.Term > rf.currentTerm {
		debugLogForRequest(rf, args.TraceId, "Received RequestVote response from server with higher term. Reset state to follower")

		rf.resetToFollower(reply.Term)
		return
	}

	rf.updateVotesReceived(server, args, reply)
}

func (rf *Raft) updateVotesReceived(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.requestVotesResponsesReceived++

	if reply.VoteGranted {
		rf.votesReceived++

		debugLogForRequest(rf, args.TraceId, fmt.Sprintf("Received vote from %d. Votes received: %d. Max votes: %d", server, rf.votesReceived, len(rf.peers)))
	}

	if rf.votesReceived >= rf.majorityCount() {
		rf.onLeaderElection()
		return
	}

	if rf.requestVotesResponsesReceived >= len(rf.peers) {
		debugLogForRequest(rf, args.TraceId, "Candidate did not receive enough votes.")
	}
}
