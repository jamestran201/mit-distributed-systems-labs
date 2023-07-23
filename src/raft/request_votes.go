package raft

type RequestVoteArgs struct {
	Term        int
	CandidateId int
	// LastLogIndex int
	// LastLogTerm  int
	TraceId string
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}
