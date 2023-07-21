package raft

import "fmt"

type AppendEntriesClient struct {
	rf      *Raft
	server  int
	traceId string
	args    *AppendEntriesArgs
	reply   *AppendEntriesReply
}

func (c *AppendEntriesClient) Run() int {
	shouldExit := false
	withLock(&c.rf.mu, func() {
		if c.rf.state != leader {
			shouldExit = true

			debugLog(c.rf, "Exiting callAppendEntries routine because server is no longer the leader")
			return
		}

		c.makeAppendEntriesRequest()
	})

	if shouldExit {
		return terminate
	}

	c.sendAppendEntries()
	return c.handleAppendEntriesResponse()
}

func (c *AppendEntriesClient) makeAppendEntriesRequest() {
	if c.traceId == "" {
		c.traceId = generateUniqueString()
	}

	c.args = &AppendEntriesArgs{
		Term:         c.rf.currentTerm,
		LeaderId:     c.rf.me,
		PrevLogIndex: c.prevLogIndexForServer(),
		PrevLogTerm:  c.prevLogTermForServer(),
		Entries:      c.logEntriesToSend(c.prevLogIndexForServer() + 1),
		LeaderCommit: c.rf.commitIndex,
		TraceId:      c.traceId,
	}
}

func (c *AppendEntriesClient) sendAppendEntries() {
	debugLogForRequestPlain(c.rf, c.args.TraceId, fmt.Sprintf("Making AppendEntries request to %d. Term %d. PrevLogIndex %d. PrevLogTerm %d. Entries %v. LeaderCommit %d.", c.server, c.args.Term, c.args.PrevLogIndex, c.args.PrevLogTerm, c.args.Entries, c.args.LeaderCommit))

	ok := c.rf.peers[c.server].Call("Raft.AppendEntries", c.args, c.reply)
	c.reply.RequestCompleted = ok
}

func (c *AppendEntriesClient) handleAppendEntriesResponse() int {
	c.rf.mu.Lock()
	defer c.rf.mu.Unlock()

	if c.rf.killed() {
		debugLogForRequest(c.rf, c.args.TraceId, "Server is killed, skip handling AppendEntries response")
		return terminate
	}

	if c.rf.state != leader {
		debugLogForRequest(c.rf, c.args.TraceId, "Server is no longer the leader, skip handling AppendEntries response")
		return terminate
	}

	if !c.reply.RequestCompleted {
		debugLogForRequest(c.rf, c.args.TraceId, "An AppendEntry request could not be processed successfully")
		return terminate
	}

	if c.reply.Term > c.rf.currentTerm {
		debugLogForRequest(c.rf, c.args.TraceId, "Received AppendEntries response from server with higher term. Reset state to follower")
		c.rf.resetToFollower(c.reply.Term)
		return terminate
	}

	if c.reply.Success {
		potentialNextIndex := c.args.PrevLogIndex + len(c.args.Entries) + 1
		if c.rf.nextIndex[c.server] > 1 && potentialNextIndex <= c.rf.nextIndex[c.server] {
			debugLogForRequest(c.rf, c.args.TraceId, fmt.Sprintf("Received a successful but stale AppendEntries response from server %d. Skip processing response. Potential index: %d. NextIndex: %d", c.server, potentialNextIndex, c.rf.nextIndex[c.server]))
			return terminate
		}

		c.rf.nextIndex[c.server] = potentialNextIndex
		c.rf.matchIndex[c.server] = c.rf.nextIndex[c.server] - 1

		debugLogForRequest(c.rf, c.args.TraceId, fmt.Sprintf("AppendEntries was successful for %d. NextIndex: %d. MatchIndex: %d", c.server, c.rf.nextIndex[c.server], c.rf.matchIndex[c.server]))

		c.rf.updateCommitIndexIfPossible(c.rf.matchIndex[c.server])

		return success
	} else {
		if c.args.PrevLogIndex < c.prevLogIndexForServer() {
			debugLogForRequest(c.rf, c.args.TraceId, fmt.Sprintf("Received a falied but stale AppendEntries response from server %d. Skip processing response.", c.server))
			return terminate
		}

		debugLogForRequest(c.rf, c.args.TraceId, fmt.Sprintf("AppendEntries was unsuccessful for server %d", c.server))

		c.rf.nextIndex[c.server] = c.reply.FirstConflictingIndex

		debugLogForRequest(c.rf, c.args.TraceId, fmt.Sprintf("First conflicting index for server %d is %d", c.server, c.reply.FirstConflictingIndex))
		debugLogForRequest(c.rf, c.args.TraceId, fmt.Sprintf("Leader logs: %v", c.rf.logs.entries))

		if c.rf.logs.snapshot != nil && c.rf.logs.entryAt(c.rf.nextIndex[c.server]) == nil {
			return trigger_install_snapshot
		}

		return failure
	}
}

func (c *AppendEntriesClient) prevLogIndexForServer() int {
	index := c.rf.nextIndex[c.server] - 1
	if index < c.rf.logs.minIndex() {
		index = c.rf.logs.snapshot.LastLogIndex
	}

	return index
}

func (c *AppendEntriesClient) prevLogTermForServer() int {
	index := c.prevLogIndexForServer()
	if c.rf.logs.snapshot != nil && index == c.rf.logs.snapshot.LastLogIndex {
		return c.rf.logs.snapshot.LastLogTerm
	}

	entry := c.rf.logs.entryAt(index)
	// if entry == nil {
	// 	debugLog(rf, fmt.Sprintf("No log entry found for index %d\nLogs: %v\nNext Indices: %v\nMatch Indices: %v", prevLogIndexForServer(rf, server), rf.logs.entries, rf.nextIndex, rf.matchIndex))
	// }

	return entry.Term
}

func (c *AppendEntriesClient) logEntriesToSend(startIndex int) []*LogEntry {
	if startIndex == 0 {
		return []*LogEntry{}
	}

	return c.rf.logs.startingFrom(startIndex)
}
