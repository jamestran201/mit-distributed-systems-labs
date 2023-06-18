package raft

import (
	"fmt"
)

const (
	success = iota
	failure
	terminate
)

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []*LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term                  int
	Success               bool
	RequestCompleted      bool
	FirstConflictingIndex int
}

func handleAppendEntries(rf *Raft, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	debugLog(rf, fmt.Sprintf("Received AppendEntries from %d", args.LeaderId))

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	rf.receivedRpcFromPeer = true

	if args.Term > rf.currentTerm || rf.state == candidate {
		debugLog(rf, "Received AppendEntries from server with higher term. Reset state to follower")
		rf.resetToFollower(args.Term)
	}

	if args.PrevLogIndex == 0 && rf.logs.lastLogIndex > 0 {
		debugLog(rf, fmt.Sprintf("BIG WARNING!!! This server contains logs while leader %d has none. This should not happen!", args.LeaderId))
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.FirstConflictingIndex = rf.logs.lastLogIndex
		return
	}

	if args.PrevLogIndex > 0 {
		log := rf.logs.entryAt(args.PrevLogIndex)
		if log == nil {
			debugLog(rf, fmt.Sprintf("The current server does not have any logs at index %d", args.PrevLogIndex))
			reply.Term = rf.currentTerm
			reply.Success = false
			reply.FirstConflictingIndex = rf.logs.lastLogIndex + 1
			return
		}

		if log.Term != args.PrevLogTerm {
			debugLog(rf, fmt.Sprintf("The logs from current server does not have the same term as leader %d", args.LeaderId))
			reply.Term = rf.currentTerm
			reply.Success = false
			reply.FirstConflictingIndex = rf.logs.firstIndexOfTerm(log.Term)
			return
		}
	}

	if len(args.Entries) > 0 {
		rf.logs.overwriteLogs(args.PrevLogIndex+1, args.Entries)
		debugLog(rf, fmt.Sprintf("Reconciled logs. Last log index %d. Last log term %d. Current term %d.", rf.logs.lastLogIndex, rf.logs.lastLogTerm, rf.currentTerm))
	} else if len(args.Entries) == 0 && rf.logs.lastLogIndex > args.PrevLogIndex {
		rf.logs.overwriteLogs(args.PrevLogIndex+1, args.Entries)
		debugLog(rf, fmt.Sprintf("Reconciled logs. Last log index %d. Last log term %d. Current term %d.", rf.logs.lastLogIndex, rf.logs.lastLogTerm, rf.currentTerm))
	}

	if args.LeaderCommit > rf.commitIndex {
		prevCommitIndex := rf.commitIndex
		if args.LeaderCommit > rf.logs.lastLogIndex {
			rf.commitIndex = rf.logs.lastLogIndex
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		debugLog(rf, fmt.Sprintf("Commit index updated to %d", rf.commitIndex))

		rf.notifyServiceOfCommittedLog(prevCommitIndex)
	}

	reply.Term = rf.currentTerm
	reply.Success = true
}

func callAppendEntries(rf *Raft, server int, isHeartbeat bool) {
	if rf.killed() {
		return
	}

	shouldExit := false
	var args *AppendEntriesArgs
	withLock(&rf.mu, func() {
		if rf.state != leader {
			shouldExit = true

			debugLog(rf, "Exiting callAppendEntries routine because server is no longer the leader")
			return
		}

		args = makeAppendEntriesRequest(rf, server)
	})

	if shouldExit {
		return
	}

	reply := &AppendEntriesReply{}
	sendAppendEntries(rf, server, args, reply)
	resultCode := handleAppendEntriesResponse(rf, server, args, reply)
	if resultCode == failure && !isHeartbeat {
		go callAppendEntries(rf, server, isHeartbeat)
	}
}

func makeAppendEntriesRequest(rf *Raft, server int) *AppendEntriesArgs {
	return &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndexForServer(rf, server),
		PrevLogTerm:  prevLogTermForServer(rf, server),
		Entries:      logEntriesToSend(rf, server),
		LeaderCommit: rf.commitIndex,
	}
}

func prevLogIndexForServer(rf *Raft, server int) int {
	return rf.nextIndex[server] - 1
}

func prevLogTermForServer(rf *Raft, server int) int {
	logEntry := rf.logs.entryAt(prevLogIndexForServer(rf, server))
	if logEntry == nil {
		return 0
	} else {
		return logEntry.Term
	}
}

func logEntriesToSend(rf *Raft, server int) []*LogEntry {
	return rf.logs.startingFrom(rf.nextIndex[server])
}

func sendAppendEntries(rf *Raft, server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	debugLog(rf, fmt.Sprintf("Making AppendEntries request to %d. Term %d. PrevLogIndex %d. PrevLogTerm %d. Entries %v. LeaderCommit %d", server, args.Term, args.PrevLogIndex, args.PrevLogTerm, args.Entries, args.LeaderCommit))

	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	reply.RequestCompleted = ok
}

func handleAppendEntriesResponse(rf *Raft, server int, args *AppendEntriesArgs, reply *AppendEntriesReply) int {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() {
		debugLog(rf, "Server is killed, skip handling AppendEntries response")
		return terminate
	}

	if rf.state != leader {
		debugLog(rf, "Server is no longer the leader, skip handling AppendEntries response")
		return terminate
	}

	if !reply.RequestCompleted {
		debugLog(rf, "An AppendEntry request could not be processed successfully")
		return terminate
	}

	if reply.Term > rf.currentTerm {
		debugLog(rf, "Received AppendEntries response from server with higher term. Reset state to follower")
		rf.resetToFollower(reply.Term)
		return terminate
	}

	if reply.Success {
		if args.PrevLogIndex < prevLogIndexForServer(rf, server) {
			debugLog(rf, fmt.Sprintf("Received a stale AppendEntries response from server %d. Skip processing response.", server))
			return terminate
		}

		rf.nextIndex[server] += len(args.Entries)
		rf.matchIndex[server] = rf.nextIndex[server] - 1
		debugLog(rf, fmt.Sprintf("AppendEntries was successful for %d. NextIndex: %d. MatchIndex: %d", server, rf.nextIndex[server], rf.matchIndex[server]))

		rf.updateCommitIndexIfPossible(rf.matchIndex[server])

		return success
	} else {
		debugLog(rf, fmt.Sprintf("AppendEntries was unsuccessful for server %d", server))
		if reply.FirstConflictingIndex > 0 && reply.FirstConflictingIndex != rf.matchIndex[server] {
			rf.nextIndex[server] = reply.FirstConflictingIndex
			debugLog(rf, fmt.Sprintf("First conflicting index for server %d is %d", server, reply.FirstConflictingIndex))
			debugLog(rf, fmt.Sprintf("logs: %v", rf.logs.entries))
		}

		return failure
	}
}
