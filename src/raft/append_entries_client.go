package raft

import "fmt"

func (rf *Raft) callAppendEntries(server int, traceId string) {
	var args *AppendEntriesArgs

	shouldExit := false
	withLock(&rf.mu, func() {
		if rf.killed() {
			debugLog(rf, "Sever is killed, aborting callAppendEntries routine.")
			shouldExit = true
			return
		}

		if rf.state != LEADER {
			debugLog(rf, "Leader state changed, aborting callAppendEntries routine.")
			shouldExit = true
			return
		}

		// TODO: Look into how this should work
		if rf.lastLogIndex < rf.nextIndex[server] {
			debugLog(rf, "Follower logs are up to date, aborting callAppendEntries routine.")
			shouldExit = true
			return
		}

		args = rf.makeAppendEntriesArgs(server, traceId)
	})

	if shouldExit {
		return
	}

	reply := &AppendEntriesReply{}
	ok := rf.sendAppendEntries(server, args, reply)
	if !ok {
		debugLogForRequestPlain(rf, args.TraceId, fmt.Sprintf("An AppendEntries request to %d could not be processed successfully, skipping", server))
		return
	}

	rf.handleAppendEntriesResponse(server, args, reply)
}

func (rf *Raft) makeAppendEntriesArgs(server int, traceId string) *AppendEntriesArgs {
	prevLogIndex := rf.nextIndex[server] - 1

	if prevLogIndex < 0 {
		debugLog(rf, fmt.Sprintf("WARNING: prevLogIndex is %d, but should be >= 0. This is not right. Server: %d. NextIndex: %+v", prevLogIndex, server, rf.nextIndex))
		panic(1)
	}

	prevLogTerm := rf.logs[prevLogIndex].Term

	entries := []LogEntry{}
	for i := prevLogIndex + 1; i <= rf.lastLogIndex; i++ {
		entry := LogEntry{Command: rf.logs[i].Command, Term: rf.logs[i].Term}
		entries = append(entries, entry)
	}

	if traceId == "" {
		traceId = generateTraceId()
	}

	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
		TraceId:      traceId,
	}

	return args
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	debugLogForRequestPlain(rf, args.TraceId, fmt.Sprintf("Sending AppendEntries to %d. Args: %+v", server, args))

	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) handleAppendEntriesResponse(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	debugLogForRequest(rf, args.TraceId, fmt.Sprintf("Received AppendEntries response from %d", server))

	if rf.killed() || rf.state != LEADER {
		debugLogForRequest(rf, args.TraceId, "State is outdated, exiting handleAppendEntriesResponse routine")
		return
	}

	if reply.Term > rf.currentTerm {
		debugLogForRequest(rf, args.TraceId, "Received Append response from server with higher term. Reset state to follower")

		rf.resetToFollower(reply.Term)
		return
	}

	if reply.Success {
		debugLogForRequest(rf, args.TraceId, fmt.Sprintf("Received successful AppendEntries response from %d", server))

		rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
		rf.matchIndex[server] = rf.nextIndex[server] - 1

		debugLogForRequest(rf, args.TraceId, fmt.Sprintf("Updated nextIndex to %d and matchIndex to %d", rf.nextIndex[server], rf.matchIndex[server]))

		rf.updateCommitIndexForLeader(rf.matchIndex[server], args.TraceId)
	} else {
		debugLogForRequest(rf, args.TraceId, fmt.Sprintf("Received failed AppendEntries response from %d", server))

		rf.nextIndex[server]--
		go rf.callAppendEntries(server, args.TraceId)
	}
}

func (rf *Raft) updateCommitIndexForLeader(index int, traceId string) {
	if index <= rf.commitIndex {
		return
	}

	if rf.logs[index].Term != rf.currentTerm {
		return
	}

	// start at 1 because we know that the current server already has a log at index based on the condition above
	replicatedCount := 1
	for s := range rf.peers {
		if s == rf.me {
			continue
		}

		if rf.matchIndex[s] >= index {
			replicatedCount++
		}
	}

	if replicatedCount > rf.majorityCount() {
		rf.commitIndex = index
		debugLogForRequest(rf, traceId, fmt.Sprintf("Commit index updated for leader to %d. Logs %v.", rf.commitIndex, rf.logs))

		rf.applyCond.Signal()
	}
}
