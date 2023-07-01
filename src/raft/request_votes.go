package raft

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
	TraceId      string
}

type RequestVoteReply struct {
	Term             int
	VoteGranted      bool
	RequestCompleted bool
}

func requestVotesFromPeers(rf *Raft, term int) {
	debugLog(rf, "Requesting votes")

	for i := 0; i < len(rf.peers); i++ {
		if rf.killed() {
			return
		}

		if i == rf.me {
			continue
		}

		go requestVotesFromServer(rf, term, i)
	}
}

func requestVotesFromServer(rf *Raft, term int, server int) {
	if server == rf.me {
		return
	}

	client := &RequestVotesClient{rf, &RequestVoteArgs{}, &RequestVoteReply{}, term, server}
	client.Run()
}
