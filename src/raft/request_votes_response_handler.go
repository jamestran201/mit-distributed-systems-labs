package raft

import "fmt"

type RequestVotesResponseHandler struct {
	rf     *Raft
	args   *RequestVoteArgs
	reply  *RequestVoteReply
	term   int
	server int
}

func (h *RequestVotesResponseHandler) Run() {
	// debugLog(rf, fmt.Sprintf("Waiting for RequestVote response. Current term: %d, Votes: %d, Responses received: %d", rf.currentTerm, votes, responsesReceived))

	h.rf.mu.Lock()
	defer h.rf.mu.Unlock()

	if h.rf.state != candidate || h.rf.currentTerm != h.term {
		debugLogForRequest(h.rf, h.args.TraceId, "State is outdated, exiting requestVotes routine")
		return
	}

	debugLogForRequest(h.rf, h.args.TraceId, fmt.Sprintf("Received RequestVote response from %d", h.server))

	if !h.reply.RequestCompleted {
		debugLogForRequest(h.rf, h.args.TraceId, "A RequestVote request could not be processed successfully, skipping")
		return
	}

	if h.reply.Term > h.rf.currentTerm {
		debugLogForRequest(h.rf, h.args.TraceId, "Received RequestVote response from server with higher term. Reset state to follower")
		h.rf.resetToFollower(h.reply.Term)
		return
	}

	h.updateVotesReceived()
}

func (h *RequestVotesResponseHandler) updateVotesReceived() {
	h.rf.requestVotesResponsesReceived++

	if h.reply.VoteGranted {
		h.rf.votesReceived++
		debugLogForRequest(h.rf, h.args.TraceId, fmt.Sprintf("Received vote from %d", h.server))
	}

	if h.rf.votesReceived > h.rf.majorityCount() {
		onLeaderElection(h.rf)
	} else if h.rf.requestVotesResponsesReceived >= len(h.rf.peers)-1 {
		debugLogForRequest(h.rf, h.args.TraceId, fmt.Sprintf("Candidate did not receive enough votes to become leader. Current term: %d", h.rf.currentTerm))
	}
}
