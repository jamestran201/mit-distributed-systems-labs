package raft

const (
	success = iota
	failure
	terminate
	trigger_install_snapshot
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

func callAppendEntries(rf *Raft, server int, isHeartbeat bool, traceId string) {
	if rf.killed() {
		return
	}

	client := &AppendEntriesClient{rf, server, traceId, &AppendEntriesArgs{}, &AppendEntriesReply{}}
	result := client.Run()
	if result == failure && !isHeartbeat {
		go callAppendEntries(rf, server, isHeartbeat, traceId)
	} else if result == trigger_install_snapshot {
		go callInstallSnapshot(rf, server, traceId)
	}
}
