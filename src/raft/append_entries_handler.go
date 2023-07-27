package raft

import "fmt"

func (rf *Raft) handleAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() {
		return
	}

	if args.Term < rf.currentTerm {
		debugLogForRequest(rf, args.TraceId, fmt.Sprintf("Rejected AppendEntries from %d because of stale incoming term. Argument term: %d", args.LeaderId, args.Term))

		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	rf.receivedRpcFromPeer = true

	if args.Term > rf.currentTerm {
		debugLogForRequest(rf, args.TraceId, fmt.Sprintf("Reset to follower because of higher incoming term. Argument term: %d", args.Term))

		rf.resetToFollower(args.Term)
	}

	if rf.state == CANDIDATE {
		debugLogForRequest(rf, args.TraceId, fmt.Sprintf("Reset to follower because received AppendEntries from leader. Current state: %s", rf.state))

		rf.resetToFollower(args.Term)
	}

	res, reason := rf.hasLogWithIndexAndTerm(args.PrevLogIndex, args.PrevLogTerm)
	if !res {
		if reason == "no_logs_at_index" {
			debugLogForRequest(rf, args.TraceId, fmt.Sprintf("Rejected AppendEntries from %d because no logs at prevLogIndex. PrevLogIndex: %d. PrevLogTerm: %d", args.LeaderId, args.PrevLogIndex, args.PrevLogTerm))
		} else {
			debugLogForRequest(rf, args.TraceId, fmt.Sprintf("Rejected AppendEntries from %d because terms do not match. PrevLogIndex: %d. PrevLogTerm: %d", args.LeaderId, args.PrevLogIndex, args.PrevLogTerm))
		}

		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	rf.resolveConflictAndAppendNewLogs(args)

	if args.LeaderCommit > rf.commitIndex {
		newCommitIndex := args.LeaderCommit
		if args.LeaderCommit > rf.lastLogIndex {
			newCommitIndex = rf.lastLogIndex
		}

		debugLogForRequest(rf, args.TraceId, fmt.Sprintf("Updating commitIndex to %d", newCommitIndex))

		rf.commitIndex = newCommitIndex
		rf.applyCond.Signal()
	}

	reply.Term = rf.currentTerm
	reply.Success = true
}

func (rf *Raft) resolveConflictAndAppendNewLogs(args *AppendEntriesArgs) {
	conflictIndex, firstNewIndex := rf.findFirstConflictIndex(args)

	if conflictIndex != -1 && firstNewIndex == -1 {
		debugLogForRequest(rf, args.TraceId, fmt.Sprintf("WARNING: ConflictIndex is %d, but FirstNewIndex is -1. This is not right", conflictIndex))
	}

	if conflictIndex != -1 {
		debugLogForRequest(rf, args.TraceId, fmt.Sprintf("Deleting logs from index %d because of conflict with leader", conflictIndex))

		res := rf.deleteAllLogsFrom(conflictIndex)
		if res != nil {
			debugLogForRequest(rf, args.TraceId, fmt.Sprintf("Error deleting logs from index %d. Error: %s", conflictIndex, res.Error()))
			panic(1)
		}
	} else {
		debugLogForRequest(rf, args.TraceId, "No conflict with leader")
	}

	if firstNewIndex != -1 {
		debugLogForRequest(rf, args.TraceId, fmt.Sprintf("Appending new entries from index %d", firstNewIndex))
		rf.appendNewEntries(args, firstNewIndex)
	} else {
		debugLogForRequest(rf, args.TraceId, "No new entries to append")
	}
}
