package raft

import "fmt"

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	// Your data here (2A).
	Term             int
	VoteGranted      bool
	RequestCompleted bool
}

func handleRequestVotes(rf *Raft, args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.log(fmt.Sprintf("Received RequestVote from %d", args.CandidateId))

	if args.Term < rf.currentTerm {
		rf.log(fmt.Sprintf("Rejected RequestVote from %d because of stale term", args.CandidateId))

		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	} else if args.Term > rf.currentTerm {
		rf.log("Received RequestVote from server with higher term. Reset state to follower")
		rf.resetToFollower(args.Term)
	}

	rf.receivedRpcFromPeer = true

	if rf.votedFor == args.CandidateId {
		reply.Term = rf.currentTerm
		reply.VoteGranted = true

		rf.log(fmt.Sprintf("Already voted for %d", args.CandidateId))
		return
	}

	if rf.votedFor == -1 && rf.logsAtLeastUpToDate(args.LastLogIndex, args.LastLogTerm) {
		rf.votedFor = args.CandidateId

		reply.Term = rf.currentTerm
		reply.VoteGranted = true

		rf.log(fmt.Sprintf("Voted for %d", args.CandidateId))
		return
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	rf.log(fmt.Sprintf("Rejected RequestVote from %d because it already voted for %d", args.CandidateId, rf.votedFor))
}

func requestVotesFromServer(rf *Raft, term int, server int) {
	if server == rf.me {
		return
	}

	if rf.killed() {
		return
	}

	shouldExit := false
	args := &RequestVoteArgs{}
	withLock(&rf.mu, func() {
		if rf.currentTerm != term || rf.state != candidate {
			rf.log(fmt.Sprintf(
				"Candidate state changed, aborting requestVotes routine.\nCurrent term: %d, given term: %d.\nCurrent state: %d", rf.currentTerm, term, rf.state,
			))
			shouldExit = true
			return
		}

		lastLogTerm := 0
		if len(rf.logs) > 0 {
			lastLogTerm = rf.logs[len(rf.logs)-1].Term
		}

		args.Term = term
		args.CandidateId = rf.me
		args.LastLogIndex = len(rf.logs)
		args.LastLogTerm = lastLogTerm
	})

	if shouldExit {
		return
	}

	reply := &RequestVoteReply{}
	sendRequestVote(rf, server, args, reply)
	handleRequestVoteResponse(rf, term, server, reply)
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
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
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func sendRequestVote(rf *Raft, server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.log(fmt.Sprintf("Sending RequestVote to %d", server))

	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	reply.RequestCompleted = ok
}

func handleRequestVoteResponse(rf *Raft, term int, server int, reply *RequestVoteReply) {
	// rf.log(fmt.Sprintf("Waiting for RequestVote response. Current term: %d, Votes: %d, Responses received: %d", rf.currentTerm, votes, responsesReceived))

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != candidate || rf.currentTerm != term {
		rf.log("State is outdated, exiting requestVotes routine")
		return
	}

	rf.log(fmt.Sprintf("Received RequestVote response from %d", server))

	if !reply.RequestCompleted {
		rf.log("A RequestVote request could not be processed successfully, skipping")
		return
	}

	if reply.Term > rf.currentTerm {
		rf.log("Received RequestVote response from server with higher term. Reset state to follower")
		rf.resetToFollower(reply.Term)
		return
	}

	updateVotesReceived(rf, reply.VoteGranted, server)
}

func updateVotesReceived(rf *Raft, voteGranted bool, server int) {
	if voteGranted {
		rf.votesReceived++
		rf.log(fmt.Sprintf("Received vote from %d", server))
	}

	if rf.votesReceived > len(rf.peers)/2 {
		rf.state = leader
		go rf.appendEntriesFanout(rf.currentTerm)

		rf.log(fmt.Sprintf("Promoted to leader. Current term: %d", rf.currentTerm))
	} else if rf.requestVotesResponsesReceived >= len(rf.peers)-1 {
		rf.log(fmt.Sprintf("Candidate did not receive enough votes to become leader. Current term: %d", rf.currentTerm))
	}
}
