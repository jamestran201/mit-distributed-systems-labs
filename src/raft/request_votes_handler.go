package raft

import "fmt"

func handleRequestVotes(rf *Raft, args *RequestVoteArgs, reply *RequestVoteReply) {
	handler := &RequestVotesHandler{rf, args, reply}
	handler.Run()
}

type RequestVotesHandler struct {
	rf    *Raft
	args  *RequestVoteArgs
	reply *RequestVoteReply
}

func (h *RequestVotesHandler) Run() {
	h.rf.mu.Lock()
	defer h.rf.mu.Unlock()

	debugLogForRequest(h.rf, h.args.TraceId, fmt.Sprintf("Received RequestVote from %d", h.args.CandidateId))

	if h.args.Term < h.rf.currentTerm {
		debugLogForRequest(h.rf, h.args.TraceId, fmt.Sprintf("Rejected RequestVote from %d because of stale term", h.args.CandidateId))

		h.reply.Term = h.rf.currentTerm
		h.reply.VoteGranted = false
		return
	}

	if h.args.Term > h.rf.currentTerm {
		debugLogForRequest(h.rf, h.args.TraceId, "Received RequestVote from server with higher term. Reset state to follower")
		h.rf.resetToFollower(h.args.Term)
	}

	if h.rf.votedFor == h.args.CandidateId {
		h.reply.Term = h.rf.currentTerm
		h.reply.VoteGranted = true

		debugLogForRequest(h.rf, h.args.TraceId, fmt.Sprintf("Already voted for %d", h.args.CandidateId))
		return
	}

	if h.rf.votedFor == -1 && h.candidateAtLeastUpToDate() {
		h.grantVote()
		return
	}

	h.reply.Term = h.rf.currentTerm
	h.reply.VoteGranted = false

	if h.rf.votedFor != -1 && h.rf.votedFor != h.args.CandidateId {
		debugLogForRequest(h.rf, h.args.TraceId, fmt.Sprintf("Rejected RequestVote from %d because it already voted for %d", h.args.CandidateId, h.rf.votedFor))
	} else if !h.candidateAtLeastUpToDate() {
		debugLogForRequest(h.rf, h.args.TraceId, fmt.Sprintf("Rejected RequestVote from %d because it's log is not at least up to date", h.args.CandidateId))
	} else {
		debugLogForRequest(h.rf, h.args.TraceId, fmt.Sprintf("Rejected RequestVote from %d for unknown reason", h.args.CandidateId))
	}
}

func (h *RequestVotesHandler) candidateAtLeastUpToDate() bool {
	if h.args.LastLogTerm == h.rf.logs.lastLogTerm {
		return h.args.LastLogIndex >= h.rf.logs.lastLogIndex
	} else {
		return h.args.LastLogTerm > h.rf.logs.lastLogTerm
	}
}

func (h *RequestVotesHandler) grantVote() {
	h.rf.receivedRpcFromPeer = true
	h.rf.votedFor = h.args.CandidateId

	h.rf.persist(nil)

	h.reply.Term = h.rf.currentTerm
	h.reply.VoteGranted = true

	debugLogForRequest(h.rf, h.args.TraceId, fmt.Sprintf("Voted for %d", h.args.CandidateId))
}
