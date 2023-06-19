package raft

import (
	"crypto/rand"
	"encoding/base64"
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
	TraceId      string
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

	debugLogForRequest(rf, args.TraceId, fmt.Sprintf("Received AppendEntries from %d", args.LeaderId))

	if args.Term < rf.currentTerm {
		debugLogForRequest(rf, args.TraceId, fmt.Sprintf("Rejected AppendEntries from %d because of stale term. Current term: %d. Given term: %d", args.LeaderId, rf.currentTerm, args.Term))

		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	rf.receivedRpcFromPeer = true

	if args.Term > rf.currentTerm || rf.state == candidate {
		debugLogForRequest(rf, args.TraceId, "Received AppendEntries from server with higher term. Reset state to follower")
		rf.resetToFollower(args.Term)
	}

	if args.PrevLogIndex == 0 && rf.logs.lastLogIndex > 0 {
		debugLogForRequest(rf, args.TraceId, fmt.Sprintf("Received AppendEntries with potentially stale PrevLogIndex. PrevLogIndex: 0. LastLogIndex: %d", rf.logs.lastLogIndex))

		reply.Term = rf.currentTerm
		reply.Success = false
		reply.FirstConflictingIndex = rf.logs.lastLogIndex
		return
	}

	if args.PrevLogIndex > 0 {
		log := rf.logs.entryAt(args.PrevLogIndex)
		if log == nil {
			debugLogForRequest(rf, args.TraceId, fmt.Sprintf("The current server does not have any logs at index %d", args.PrevLogIndex))

			reply.Term = rf.currentTerm
			reply.Success = false
			reply.FirstConflictingIndex = rf.logs.lastLogIndex + 1
			return
		}

		if log.Term != args.PrevLogTerm {
			debugLogForRequest(rf, args.TraceId, fmt.Sprintf("The logs from current server does not have the same term as leader %d. Current log term: %d. PrevLogTerm: %d", args.LeaderId, log.Term, args.PrevLogTerm))

			reply.Term = rf.currentTerm
			reply.Success = false
			reply.FirstConflictingIndex = rf.logs.firstIndexOfTerm(log.Term)
			return
		}
	}

	if len(args.Entries) > 0 {
		rf.logs.overwriteLogs(args.PrevLogIndex+1, args.Entries)

		debugLogForRequest(rf, args.TraceId, fmt.Sprintf("Reconciled logs. Last log index %d. Last log term %d. Current term %d.", rf.logs.lastLogIndex, rf.logs.lastLogTerm, rf.currentTerm))
	} else if len(args.Entries) == 0 && rf.logs.lastLogIndex > args.PrevLogIndex {
		rf.logs.overwriteLogs(args.PrevLogIndex+1, args.Entries)

		debugLogForRequest(rf, args.TraceId, fmt.Sprintf("Reconciled logs. Last log index %d. Last log term %d. Current term %d.", rf.logs.lastLogIndex, rf.logs.lastLogTerm, rf.currentTerm))
	}

	if args.LeaderCommit > rf.commitIndex {
		prevCommitIndex := rf.commitIndex
		if args.LeaderCommit > rf.logs.lastLogIndex {
			rf.commitIndex = rf.logs.lastLogIndex
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		debugLogForRequest(rf, args.TraceId, fmt.Sprintf("Commit index updated to %d", rf.commitIndex))

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
		TraceId:      generateUniqueString(),
	}
}

func generateUniqueString() string {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	if err != nil {
		panic(err)
	}
	return base64.URLEncoding.EncodeToString(b)
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
	debugLogForRequest(rf, args.TraceId, fmt.Sprintf("Making AppendEntries request to %d. Term %d. PrevLogIndex %d. PrevLogTerm %d. Entries %v. LeaderCommit %d.", server, args.Term, args.PrevLogIndex, args.PrevLogTerm, args.Entries, args.LeaderCommit))

	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	reply.RequestCompleted = ok
}

func handleAppendEntriesResponse(rf *Raft, server int, args *AppendEntriesArgs, reply *AppendEntriesReply) int {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() {
		debugLogForRequest(rf, args.TraceId, "Server is killed, skip handling AppendEntries response")
		return terminate
	}

	if rf.state != leader {
		debugLogForRequest(rf, args.TraceId, "Server is no longer the leader, skip handling AppendEntries response")
		return terminate
	}

	if !reply.RequestCompleted {
		debugLogForRequest(rf, args.TraceId, "An AppendEntry request could not be processed successfully")
		return terminate
	}

	if reply.Term > rf.currentTerm {
		debugLogForRequest(rf, args.TraceId, "Received AppendEntries response from server with higher term. Reset state to follower")
		rf.resetToFollower(reply.Term)
		return terminate
	}

	if reply.Success {
		if args.PrevLogIndex < prevLogIndexForServer(rf, server) {
			debugLogForRequest(rf, args.TraceId, fmt.Sprintf("Received a stale AppendEntries response from server %d. Skip processing response.", server))
			return terminate
		}

		rf.nextIndex[server] += len(args.Entries)
		rf.matchIndex[server] = rf.nextIndex[server] - 1
		debugLogForRequest(rf, args.TraceId, fmt.Sprintf("AppendEntries was successful for %d. NextIndex: %d. MatchIndex: %d", server, rf.nextIndex[server], rf.matchIndex[server]))

		rf.updateCommitIndexIfPossible(rf.matchIndex[server])

		return success
	} else {
		debugLog(rf, fmt.Sprintf("AppendEntries was unsuccessful for server %d", server))
		if reply.FirstConflictingIndex > 0 && reply.FirstConflictingIndex != rf.matchIndex[server] {
			rf.nextIndex[server] = reply.FirstConflictingIndex
			debugLogForRequest(rf, args.TraceId, fmt.Sprintf("First conflicting index for server %d is %d", server, reply.FirstConflictingIndex))
			debugLogForRequest(rf, args.TraceId, fmt.Sprintf("Leader logs: %v", rf.logs.entries))
		}

		return failure
	}
}
