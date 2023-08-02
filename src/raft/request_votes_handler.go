package raft

import "fmt"

func (rf *Raft) handleRequestVotes(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() {
		return
	}

	debugLogForRequest(rf, args.TraceId, fmt.Sprintf("Received RequestVotes from %d", args.CandidateId))

	if args.Term < rf.currentTerm {
		debugLogForRequest(rf, args.TraceId, fmt.Sprintf("Rejected RequestVotes from %d because of stale incoming term. Argument term: %d", args.CandidateId, args.Term))

		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		debugLogForRequest(rf, args.TraceId, fmt.Sprintf("Converting to follower because of higher incoming term. Argument term: %d", args.Term))

		rf.resetToFollower(args.Term)
	}

	isCandidateLogUpToDate := rf.isCandidateLogUpToDate(args)
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && isCandidateLogUpToDate {
		debugLogForRequest(rf, args.TraceId, fmt.Sprintf("Voted for %d", args.CandidateId))
		rf.votedFor = args.CandidateId
		rf.receivedRpcFromPeer = true

		rf.persist()

		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		return
	}

	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		debugLogForRequest(rf, args.TraceId, fmt.Sprintf("Rejected RequestVotes from %d because already voted for %d", args.CandidateId, rf.votedFor))
	} else if !isCandidateLogUpToDate {
		debugLogForRequest(rf, args.TraceId, fmt.Sprintf("Rejected RequestVotes from %d because candidate log is not up to date", args.CandidateId))
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false
}

func (rf *Raft) isCandidateLogUpToDate(args *RequestVoteArgs) bool {
	if rf.lastLogTerm != args.LastLogTerm {
		return args.LastLogTerm > rf.lastLogTerm
	}

	return args.LastLogIndex >= rf.lastLogIndex
}
