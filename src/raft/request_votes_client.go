package raft

import "fmt"

type RequestVotesClient struct {
	rf     *Raft
	args   *RequestVoteArgs
	reply  *RequestVoteReply
	term   int
	server int
}

func (c *RequestVotesClient) Run() {
	shouldExit := false
	withLock(&c.rf.mu, func() {
		if c.rf.killed() {
			shouldExit = true
			return
		}

		if c.rf.currentTerm != c.term || c.rf.state != candidate {
			debugLog(c.rf, fmt.Sprintf(
				"Candidate state changed, aborting requestVotes routine.\nCurrent term: %d, given term: %d.\nCurrent state: %d", c.rf.currentTerm, c.term, c.rf.state,
			))
			shouldExit = true
			return
		}

		c.makeRequest()
	})

	if shouldExit {
		return
	}

	c.sendRequestVote()
	c.handleResponse()
}

func (c *RequestVotesClient) makeRequest() {
	c.args.Term = c.term
	c.args.CandidateId = c.rf.me
	c.args.LastLogIndex = c.rf.logs.lastLogIndex
	c.args.LastLogTerm = c.rf.logs.lastLogTerm
	c.args.TraceId = generateUniqueString()
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
func (c *RequestVotesClient) sendRequestVote() {
	debugLogPlain(c.rf, fmt.Sprintf("Sending RequestVote to %d", c.server))

	ok := c.rf.peers[c.server].Call("Raft.RequestVote", c.args, c.reply)
	c.reply.RequestCompleted = ok
}

func (c *RequestVotesClient) handleResponse() {
	// debugLog(rf, fmt.Sprintf("Waiting for RequestVote response. Current term: %d, Votes: %d, Responses received: %d", rf.currentTerm, votes, responsesReceived))

	c.rf.mu.Lock()
	defer c.rf.mu.Unlock()

	if c.rf.state != candidate || c.rf.currentTerm != c.term {
		debugLogForRequest(c.rf, c.args.TraceId, "State is outdated, exiting requestVotes routine")
		return
	}

	debugLogForRequest(c.rf, c.args.TraceId, fmt.Sprintf("Received RequestVote response from %d", c.server))

	if !c.reply.RequestCompleted {
		debugLogForRequest(c.rf, c.args.TraceId, "A RequestVote request could not be processed successfully, skipping")
		return
	}

	if c.reply.Term > c.rf.currentTerm {
		debugLogForRequest(c.rf, c.args.TraceId, "Received RequestVote response from server with higher term. Reset state to follower")
		c.rf.resetToFollower(c.reply.Term)
		return
	}

	c.updateVotesReceived()
}

func (c *RequestVotesClient) updateVotesReceived() {
	c.rf.requestVotesResponsesReceived++

	if c.reply.VoteGranted {
		c.rf.votesReceived++
		debugLogForRequest(c.rf, c.args.TraceId, fmt.Sprintf("Received vote from %d", c.server))
	}

	if c.rf.votesReceived > c.rf.majorityCount() {
		onLeaderElection(c.rf)
	} else if c.rf.requestVotesResponsesReceived >= len(c.rf.peers)-1 {
		debugLogForRequest(c.rf, c.args.TraceId, fmt.Sprintf("Candidate did not receive enough votes to become leader. Current term: %d", c.rf.currentTerm))
	}
}
