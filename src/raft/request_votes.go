package raft

import "fmt"

type RequestVoteArgs struct {
	Term        int
	CandidateId int
	// LastLogIndex int
	// LastLogTerm  int
	TraceId string
}

type RequestVoteReply struct {
	Term             int
	VoteGranted      bool
	RequestCompleted bool
}

func (rf *Raft) handleRequestVotes(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	debugLogForRequest(rf, args.TraceId, fmt.Sprintf("Received RequestVotes from %d", args.CandidateId))

	if args.Term < rf.currentTerm {
		debugLogForRequest(rf, args.TraceId, fmt.Sprintf("Rejected RequestVotes from %d because of stale incoming term. Argument term: %d", args.CandidateId, args.Term))

		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		debugLogForRequest(rf, args.TraceId, fmt.Sprintf("Converting to follower because of higher incoming term. Argument term: %d", args.Term))

		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
	}

	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		debugLogForRequest(rf, args.TraceId, fmt.Sprintf("Rejected RequestVotes from %d because already voted for %d", args.CandidateId, rf.votedFor))

		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	debugLogForRequest(rf, args.TraceId, fmt.Sprintf("Voted for %d", args.CandidateId))
	rf.votedFor = args.CandidateId

	reply.Term = rf.currentTerm
	reply.VoteGranted = true
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
