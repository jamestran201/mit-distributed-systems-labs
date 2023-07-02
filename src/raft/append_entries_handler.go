package raft

import "fmt"

func handleAppendEntries(rf *Raft, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	handler := &AppendEntriesHandler{rf, args, reply}
	handler.Run()
}

type AppendEntriesHandler struct {
	rf    *Raft
	args  *AppendEntriesArgs
	reply *AppendEntriesReply
}

func (h *AppendEntriesHandler) Run() {
	h.rf.mu.Lock()
	defer h.rf.mu.Unlock()

	debugLogForRequest(h.rf, h.args.TraceId, fmt.Sprintf("Received AppendEntries from %d", h.args.LeaderId))

	if h.args.Term < h.rf.currentTerm {
		debugLogForRequest(h.rf, h.args.TraceId, fmt.Sprintf("Rejected AppendEntries from %d because of stale term. Given term: %d", h.args.LeaderId, h.args.Term))

		h.reply.Term = h.rf.currentTerm
		h.reply.Success = false
		return
	}

	h.rf.receivedRpcFromPeer = true

	if h.shouldResetToFollower() {
		debugLogForRequest(h.rf, h.args.TraceId, "Received AppendEntries from server with higher term. Reset state to follower")
		h.rf.resetToFollower(h.args.Term)
	}

	if !h.doesLogAtPrevLogIndexMatch() {
		return
	}

	h.reconcileLogs()

	if h.args.LeaderCommit > h.rf.commitIndex {
		h.updateCommitIndex()
	}

	h.reply.Term = h.rf.currentTerm
	h.reply.Success = true
}

func (h *AppendEntriesHandler) shouldResetToFollower() bool {
	return h.args.Term > h.rf.currentTerm || (h.args.Term == h.rf.currentTerm && h.rf.state == candidate)
}

func (h *AppendEntriesHandler) doesLogAtPrevLogIndexMatch() bool {
	log := h.rf.logs.entryAt(h.args.PrevLogIndex)
	if log == nil {
		debugLogForRequest(h.rf, h.args.TraceId, fmt.Sprintf("The current server does not have any logs at index %d", h.args.PrevLogIndex))

		h.reply.Term = h.rf.currentTerm
		h.reply.Success = false
		h.reply.FirstConflictingIndex = h.rf.logs.lastLogIndex + 1

		return false
	}

	if log.Term != h.args.PrevLogTerm {
		// debugLogForRequest(h.rf, h.args.TraceId, fmt.Sprintf("The logs from current server does not have the same term as leader %d. Current log term: %d. PrevLogTerm: %d.\nLogs: %v", h.args.LeaderId, log.Term, h.args.PrevLogTerm, h.rf.logs.entries))

		h.reply.Term = h.rf.currentTerm
		h.reply.Success = false
		h.reply.FirstConflictingIndex = h.rf.logs.firstIndexOfTerm(log.Term)

		return false
	}

	return true
}

func (h *AppendEntriesHandler) reconcileLogs() {
	debugLogForRequest(h.rf, h.args.TraceId, fmt.Sprintf("Before reconciling logs: %v", h.rf.logs.entries))

	firstConflictingIndex, lastAgreeingIndex, firstNewLogPos := h.rf.logs.findIndicesForReconciliation(h.args.PrevLogIndex+1, h.args.Entries)

	if firstConflictingIndex == 0 {
		debugLogForRequest(h.rf, h.args.TraceId, fmt.Sprintf("Found conflicting index at 0. This is not right! Current logs: %v", h.rf.logs.entries))

		panic("")
	}

	if firstConflictingIndex > 0 {
		h.rf.logs.deleteLogsFrom(firstConflictingIndex)
		h.rf.persist()

		debugLogForRequest(h.rf, h.args.TraceId, fmt.Sprintf("Found conflicting index at %d. Deleted all logs starting from this index. Current logs: %v", firstConflictingIndex, h.rf.logs.entries))
	}

	appended := h.rf.logs.appendNewEntries(lastAgreeingIndex+1, firstNewLogPos, h.args.Entries)
	if appended {
		h.rf.persist()
	}

	debugLogForRequest(h.rf, h.args.TraceId, fmt.Sprintf("After reconciling logs: %v", h.rf.logs.entries))
}

func (h *AppendEntriesHandler) updateCommitIndex() {
	prevCommitIndex := h.rf.commitIndex
	if h.args.LeaderCommit > h.rf.logs.lastLogIndex {
		h.rf.commitIndex = h.rf.logs.lastLogIndex
	} else {
		h.rf.commitIndex = h.args.LeaderCommit
	}

	h.rf.persist()

	debugLogForRequest(h.rf, h.args.TraceId, fmt.Sprintf("Commit index updated to %d. Logs: %v", h.rf.commitIndex, h.rf.logs.entries))

	h.rf.notifyServiceOfCommittedLog(prevCommitIndex)
}
