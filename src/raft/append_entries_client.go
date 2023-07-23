package raft

import "fmt"

func (rf *Raft) callAppendEntries(server int) {
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

		args = rf.makeAppendEntriesArgs()
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

func (rf *Raft) makeAppendEntriesArgs() *AppendEntriesArgs {
	args := &AppendEntriesArgs{
		Term:     rf.currentTerm,
		LeaderId: rf.me,
		TraceId:  generateTraceId(),
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
}
