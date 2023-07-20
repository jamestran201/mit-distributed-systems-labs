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

type InstallSnapshotClient struct {
	rf      *Raft
	server  int
	traceId string
	args    *InstallSnapshotArgs
	reply   *InstallSnapshotReply
}

func (c *InstallSnapshotClient) Run() {
	shouldExit := false
	withLock(&c.rf.mu, func() {
		if c.rf.state != leader {
			shouldExit = true

			debugLog(c.rf, "Exiting InstallSnapshot#Run() routine because server is no longer the leader")
			return
		}

		c.makeInstallSnapshotRequest()
	})

	if shouldExit {
		return
	}

	c.sendInstallSnapshot()
	c.handleInstallSnapshotResponse()
}

func (c *InstallSnapshotClient) makeInstallSnapshotRequest() {
	if c.traceId == "" {
		c.args.TraceId = generateUniqueString()
	}

	c.args = &InstallSnapshotArgs{
		Term:              c.rf.currentTerm,
		LeaderId:          c.rf.me,
		LastIncludedIndex: c.rf.logs.snapshot.LastLogIndex,
		LastIncludedTerm:  c.rf.logs.snapshot.LastLogTerm,
		Data:              c.rf.logs.snapshot.Data,
		TraceId:           c.args.TraceId,
	}
}

func (c *InstallSnapshotClient) sendInstallSnapshot() {
	debugLogForRequestPlain(c.rf, c.args.TraceId, fmt.Sprintf("Making InstallSnapshot request to %d. Term %d. LastIncludedIndex %d. LastIncludedTerm %d.", c.server, c.args.Term, c.args.LastIncludedIndex, c.args.LastIncludedTerm))

	ok := c.rf.peers[c.server].Call("Raft.InstallSnapshot", c.args, c.reply)
	c.reply.RequestCompleted = ok
}

func (c *InstallSnapshotClient) handleInstallSnapshotResponse() {
	c.rf.mu.Lock()
	defer c.rf.mu.Unlock()

	if c.rf.killed() {
		debugLogForRequest(c.rf, c.args.TraceId, "Server is killed, skip handling InstallSnapshot response")
	}

	if c.rf.state != leader {
		debugLogForRequest(c.rf, c.args.TraceId, "Server is no longer the leader, skip handling InstallSnapshot response")
	}

	if !c.reply.RequestCompleted {
		debugLogForRequest(c.rf, c.args.TraceId, "An InstallSnapshot request could not be processed successfully")
	}

	if c.reply.Term > c.rf.currentTerm {
		debugLogForRequest(c.rf, c.args.TraceId, "Received InstallSnapshot response from server with higher term. Reset state to follower")
		c.rf.resetToFollower(c.reply.Term)
	}
}
