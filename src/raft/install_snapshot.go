package raft

import "fmt"

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
	TraceId           string
}

type InstallSnapshotReply struct {
	Term             int
	RequestCompleted bool
}

func handleInstallSnapshot(rf *Raft, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	handler := &InstallSnapshotHandler{rf, args, reply}
	handler.Run()
}

type InstallSnapshotHandler struct {
	rf    *Raft
	args  *InstallSnapshotArgs
	reply *InstallSnapshotReply
}

func (h *InstallSnapshotHandler) Run() {
	h.rf.mu.Lock()
	defer h.rf.mu.Unlock()

	debugLogForRequest(h.rf, h.args.TraceId, fmt.Sprintf("Received InstallSnapshot from %d", h.args.LeaderId))

	h.reply.Term = h.rf.currentTerm

	if h.args.Term < h.rf.currentTerm {
		debugLogForRequest(h.rf, h.args.TraceId, fmt.Sprintf("Rejected InstallSnapshot from %d because of stale term. Given term: %d", h.args.LeaderId, h.args.Term))

		return
	}

	if h.args.LastIncludedIndex <= h.rf.logs.snapshot.LastLogIndex {
		return
	}

	// TODO: What if args and snapshot have different terms?

	newSnapshot := &Snapshot{
		LastLogIndex: h.args.LastIncludedIndex,
		LastLogTerm:  h.args.LastIncludedTerm,
		Data:         h.args.Data,
	}
	h.rf.logs.snapshot = newSnapshot
	h.rf.logs.deleteLogsUntil(h.args.LastIncludedIndex)

	msg := ApplyMsg{
		CommandValid:  false,
		Snapshot:      h.args.Data,
		SnapshotTerm:  h.args.LastIncludedTerm,
		SnapshotIndex: h.args.LastIncludedIndex,
	}
	h.rf.applyCh <- msg
}
