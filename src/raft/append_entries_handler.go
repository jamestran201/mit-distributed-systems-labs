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
}
