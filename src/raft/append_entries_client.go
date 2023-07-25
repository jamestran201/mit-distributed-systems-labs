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

	if traceId == "" {
		traceId = generateTraceId()
	}

	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		LeaderCommit: rf.commitIndex,
		TraceId:      traceId,
	}

	return args
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	debugLogPlain(rf, fmt.Sprintf("Sending AppendEntries to %d", server))

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

		// TODO: implement committing logs
	} else {
		debugLogForRequest(rf, args.TraceId, fmt.Sprintf("Received failed AppendEntries response from %d", server))

		rf.nextIndex[server]--
		go rf.callAppendEntries(server, args.TraceId)
	}
}
