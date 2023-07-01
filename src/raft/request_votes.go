package raft

import "fmt"

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
	TraceId      string
}

type RequestVoteReply struct {
	// Your data here (2A).
	Term             int
	VoteGranted      bool
	RequestCompleted bool
}

func handleRequestVotes(rf *Raft, args *RequestVoteArgs, reply *RequestVoteReply) {
	handler := &RequestVotesHandler{rf, args, reply}
	handler.Run()
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

	shouldExit := false
	args := &RequestVoteArgs{}
	withLock(&rf.mu, func() {
		if rf.killed() {
			shouldExit = true
			return
		}

		if rf.currentTerm != term || rf.state != candidate {
			debugLog(rf, fmt.Sprintf(
				"Candidate state changed, aborting requestVotes routine.\nCurrent term: %d, given term: %d.\nCurrent state: %d", rf.currentTerm, term, rf.state,
			))
			shouldExit = true
			return
		}

		args.Term = term
		args.CandidateId = rf.me
		args.LastLogIndex = rf.logs.lastLogIndex
		args.LastLogTerm = rf.logs.lastLogTerm
		args.TraceId = generateUniqueString()
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
	debugLogPlain(rf, fmt.Sprintf("Sending RequestVote to %d", server))

	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	reply.RequestCompleted = ok
}

func handleRequestVoteResponse(rf *Raft, term int, server int, reply *RequestVoteReply) {
	// debugLog(rf, fmt.Sprintf("Waiting for RequestVote response. Current term: %d, Votes: %d, Responses received: %d", rf.currentTerm, votes, responsesReceived))

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != candidate || rf.currentTerm != term {
		debugLog(rf, "State is outdated, exiting requestVotes routine")
		return
	}

	debugLog(rf, fmt.Sprintf("Received RequestVote response from %d", server))

	if !reply.RequestCompleted {
		debugLog(rf, "A RequestVote request could not be processed successfully, skipping")
		return
	}

	if reply.Term > rf.currentTerm {
		debugLog(rf, "Received RequestVote response from server with higher term. Reset state to follower")
		rf.resetToFollower(reply.Term)
		return
	}

	updateVotesReceived(rf, reply.VoteGranted, server)
}

func updateVotesReceived(rf *Raft, voteGranted bool, server int) {
	rf.requestVotesResponsesReceived++

	if voteGranted {
		rf.votesReceived++
		debugLog(rf, fmt.Sprintf("Received vote from %d", server))
	}

	if rf.votesReceived > rf.majorityCount() {
		onLeaderElection(rf)
	} else if rf.requestVotesResponsesReceived >= len(rf.peers)-1 {
		debugLog(rf, fmt.Sprintf("Candidate did not receive enough votes to become leader. Current term: %d", rf.currentTerm))
	}
}
