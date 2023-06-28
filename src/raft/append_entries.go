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
	handler := &AppendEntriesHandler{rf, args, reply}
	handler.Run()
}

func callAppendEntries(rf *Raft, server int, isHeartbeat bool, traceId string) {
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

		// TODO: Re-consider this
		if !isHeartbeat && rf.logs.lastLogIndex < rf.nextIndex[server] {
			shouldExit = true
			debugLog(rf, fmt.Sprintf("Exiting callAppendEntries routine because logs for %d is up to date", server))
		}

		args = makeAppendEntriesRequest(rf, server, traceId)
	})

	if shouldExit {
		return
	}

	reply := &AppendEntriesReply{}
	sendAppendEntries(rf, server, args, reply)
	resultCode := handleAppendEntriesResponse(rf, server, args, reply)
	if resultCode == failure && !isHeartbeat {
		go callAppendEntries(rf, server, isHeartbeat, traceId)
	}
}

func makeAppendEntriesRequest(rf *Raft, server int, traceId string) *AppendEntriesArgs {
	if traceId == "" {
		traceId = generateUniqueString()
	}

	return &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndexForServer(rf, server),
		PrevLogTerm:  prevLogTermForServer(rf, server),
		Entries:      logEntriesToSend(rf, server),
		LeaderCommit: rf.commitIndex,
		TraceId:      traceId,
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
	return rf.logs.entryAt(prevLogIndexForServer(rf, server)).Term
}

func logEntriesToSend(rf *Raft, server int) []*LogEntry {
	return rf.logs.startingFrom(rf.nextIndex[server])
}

func sendAppendEntries(rf *Raft, server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	debugLogForRequestPlain(rf, args.TraceId, fmt.Sprintf("Making AppendEntries request to %d. Term %d. PrevLogIndex %d. PrevLogTerm %d. Entries %v. LeaderCommit %d.", server, args.Term, args.PrevLogIndex, args.PrevLogTerm, args.Entries, args.LeaderCommit))

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
		debugLogForRequest(rf, args.TraceId, fmt.Sprintf("AppendEntries was unsuccessful for server %d", server))

		rf.nextIndex[server] = reply.FirstConflictingIndex

		debugLogForRequest(rf, args.TraceId, fmt.Sprintf("First conflicting index for server %d is %d", server, reply.FirstConflictingIndex))
		debugLogForRequest(rf, args.TraceId, fmt.Sprintf("Leader logs: %v", rf.logs.entries))

		return failure
	}
}
