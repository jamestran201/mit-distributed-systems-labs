package raft

import (
	"fmt"
	"time"
)

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term             int
	Success          bool
	RequestCompleted bool
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

	if args.PrevLogIndex == 0 && len(rf.logs) > 0 {
		debugLog(rf, fmt.Sprintf("BIG WARNING!!! This server contains logs while leader %d has none. This should not happen!", args.LeaderId))
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if args.PrevLogIndex > len(rf.logs) {
		debugLog(rf, fmt.Sprintf("The logs from current server are not in sync with leader %d", args.LeaderId))
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// At this point, there are 2 possibilities:
	// 1. PrevLogIndex is 0 and the server has no logs
	// 2. PrevLogIndex is > 0 and the server has at least PrevLogIndex logs
	// This condition follows case 2
	// Use PrevLogIndex - 1 because log indices start from 1
	if args.PrevLogIndex > 0 && args.PrevLogTerm != rf.logs[args.PrevLogIndex-1].Term {
		debugLog(rf, fmt.Sprintf("The logs from current server does not have the same term as leader %d", args.LeaderId))
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// The log at rf.logs[PrevLogIndex-1] is the last one on this server that matches the leader's log.
	// All logs after this differ from the leader's and should be discarded.
	if len(rf.logs) > args.PrevLogIndex {
		rf.logs = rf.logs[:args.PrevLogIndex]
	}

	rf.logs = append(rf.logs, args.Entries...)

	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit > len(rf.logs) {
			rf.commitIndex = len(rf.logs)
		} else {
			rf.commitIndex = args.LeaderCommit
		}
	}

	reply.Term = rf.currentTerm
	reply.Success = true
}

func startHeartBeat(rf *Raft) {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go sendHeartBeatLoop(rf, i)
	}
}

func sendHeartBeatLoop(rf *Raft, server int) {
	var stopCh chan bool
	for {
		if stopCh != nil {
			stopCh <- true
		}

		stopCh = make(chan bool, 1)
		go sendHeartBeat(rf, server, stopCh)

		time.Sleep(300 * time.Millisecond)
	}
}

func sendHeartBeat(rf *Raft, server int, stopChan chan bool) {
	if rf.killed() {
		return
	}

	shouldExit := false
	args := &AppendEntriesArgs{}
	withLock(&rf.mu, func() {
		if rf.state != leader {
			shouldExit = true

			debugLog(rf, "Exiting sendHeartBeat routine because server is no longer the leader")
			return
		}

		prevLogTerm := 0
		if len(rf.logs) > 0 {
			prevLogTerm = rf.logs[len(rf.logs)-1].Term
		}

		args = &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: len(rf.logs),
			PrevLogTerm:  prevLogTerm,
			Entries:      []LogEntry{},
		}
	})

	if shouldExit {
		return
	}

	debugLog(rf, fmt.Sprintf("Sending heartbeat to %d", server))

	reply := &AppendEntriesReply{}
	sendAppendEntries(rf, server, args, reply)

	shouldExit = <-stopChan
	if shouldExit {
		return
	}

	handleAppendEntriesResponse(rf, reply)
}

func sendAppendEntries(rf *Raft, server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.killed() {
		return
	}

	debugLog(rf, fmt.Sprintf("Sending AppendEntries to %d", server))

	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	reply.RequestCompleted = ok
}

func handleAppendEntriesResponse(rf *Raft, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != leader {
		debugLog(rf, "Server is no longer the leader, skip handling AppendEntries response")
		return
	}

	if !reply.RequestCompleted {
		debugLog(rf, "An AppendEntry request could not be processed successfully")
		return
	}

	if reply.Term > rf.currentTerm {
		debugLog(rf, "Received AppendEntries response from server with higher term. Reset state to follower")
		rf.resetToFollower(reply.Term)
		return
	}
}
