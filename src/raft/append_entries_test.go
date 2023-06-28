package raft

import "testing"

func TestRejectAppendEntriesWithLowerTerm(t *testing.T) {
	cfg := make_config(t, 1, false, false)
	defer cfg.cleanup()

	server := cfg.rafts[0]
	server.currentTerm = 2
	args := &AppendEntriesArgs{
		Term:         1,
		LeaderId:     2,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []*LogEntry{},
		LeaderCommit: 0,
		TraceId:      "",
	}
	reply := &AppendEntriesReply{}

	handleAppendEntries(server, args, reply)

	if reply.Success != false {
		t.Errorf("Expected success to be false. Got %v", reply.Success)
	}

	if reply.Term != server.currentTerm {
		t.Errorf("Expected term to be %d. Got %d", server.currentTerm, reply.Term)
	}
}

func TestRejectWhenEntryAtPrevLogIndexDoesNotExist(t *testing.T) {
	cfg := make_config(t, 1, false, false)
	defer cfg.cleanup()

	server := cfg.rafts[0]
	server.currentTerm = 1
	args := &AppendEntriesArgs{
		Term:         1,
		LeaderId:     2,
		PrevLogIndex: 1,
		PrevLogTerm:  1,
		Entries:      []*LogEntry{},
		LeaderCommit: 0,
		TraceId:      "",
	}
	reply := &AppendEntriesReply{}

	handleAppendEntries(server, args, reply)

	if reply.Success != false {
		t.Errorf("Expected success to be false. Got %v", reply.Success)
	}
}

func TestRejectWhenPrevLogTermDoesNotMatch(t *testing.T) {
	cfg := make_config(t, 1, false, false)
	defer cfg.cleanup()

	server := cfg.rafts[0]
	server.currentTerm = 2
	server.logs.entries[1] = &LogEntry{Command: "x", Term: 1}
	args := &AppendEntriesArgs{
		Term:         2,
		LeaderId:     2,
		PrevLogIndex: 1,
		PrevLogTerm:  2,
		Entries:      []*LogEntry{},
		LeaderCommit: 0,
		TraceId:      "",
	}
	reply := &AppendEntriesReply{}

	handleAppendEntries(server, args, reply)

	if reply.Success != false {
		t.Errorf("Expected success to be false. Got %v", reply.Success)
	}
}

func TestResolvesConflictingEntries1(t *testing.T) {
	cfg := make_config(t, 1, false, false)
	defer cfg.cleanup()

	server := cfg.rafts[0]
	server.currentTerm = 2
	server.logs.entries[0] = &LogEntry{Command: "null", Term: 0}
	server.logs.entries[1] = &LogEntry{Command: "x", Term: 1}
	server.logs.lastLogIndex = 1
	server.logs.lastLogTerm = 1

	expectedLogEntry := &LogEntry{Command: "y", Term: 2}
	args := &AppendEntriesArgs{
		Term:         2,
		LeaderId:     2,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []*LogEntry{expectedLogEntry},
		LeaderCommit: 0,
		TraceId:      "",
	}
	reply := &AppendEntriesReply{}

	handleAppendEntries(server, args, reply)

	if reply.Success != true {
		t.Errorf("Expected success to be true. Got %v", reply.Success)
	}

	logEntry := server.logs.entryAt(1)
	if logEntry.Command != expectedLogEntry.Command || logEntry.Term != expectedLogEntry.Term {
		t.Errorf("Expected log entry at index 1 to be %v. Got %v", expectedLogEntry, logEntry)
	}

	if len(server.logs.entries) != 2 {
		t.Errorf("Expected server to have 2 log entries. Got %d", len(server.logs.entries))
	}
}

func TestResolvesConflictingEntries2(t *testing.T) {
	cfg := make_config(t, 1, false, false)
	defer cfg.cleanup()

	server := cfg.rafts[0]
	server.currentTerm = 2
	server.logs.entries[0] = &LogEntry{Command: "null", Term: 0}
	server.logs.entries[1] = &LogEntry{Command: "x", Term: 1}
	server.logs.entries[2] = &LogEntry{Command: "y", Term: 1}
	server.logs.entries[3] = &LogEntry{Command: "z", Term: 1}
	server.logs.lastLogIndex = 3
	server.logs.lastLogTerm = 1

	expectedLogEntries := []*LogEntry{{Command: "a", Term: 2}, {Command: "b", Term: 2}}
	args := &AppendEntriesArgs{
		Term:         2,
		LeaderId:     2,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      expectedLogEntries,
		LeaderCommit: 0,
		TraceId:      "",
	}
	reply := &AppendEntriesReply{}

	handleAppendEntries(server, args, reply)

	if reply.Success != true {
		t.Errorf("Expected success to be true. Got %v", reply.Success)
	}

	logEntry := server.logs.entryAt(1)
	if logEntry.Command != expectedLogEntries[0].Command || logEntry.Term != expectedLogEntries[0].Term {
		t.Errorf("Expected log entry at index 1 to be %v. Got %v", expectedLogEntries[0], logEntry)
	}

	logEntry = server.logs.entryAt(2)
	if logEntry.Command != expectedLogEntries[1].Command || logEntry.Term != expectedLogEntries[1].Term {
		t.Errorf("Expected log entry at index 2 to be %v. Got %v", expectedLogEntries[1], logEntry)
	}

	if len(server.logs.entries) != 3 {
		t.Errorf("Expected server to have 3 log entries. Got %d", len(server.logs.entries))
	}
}

func TestUpdatesCommitIndex(t *testing.T) {
	cfg := make_config(t, 1, false, false)
	defer cfg.cleanup()

	server := cfg.rafts[0]
	server.currentTerm = 2
	server.logs.entries[0] = &LogEntry{Command: "null", Term: 0}
	server.logs.entries[1] = &LogEntry{Command: "x", Term: 1}
	server.logs.lastLogIndex = 1
	server.logs.lastLogTerm = 1

	args := &AppendEntriesArgs{
		Term:         2,
		LeaderId:     2,
		PrevLogIndex: 1,
		PrevLogTerm:  1,
		Entries:      []*LogEntry{},
		LeaderCommit: 1,
		TraceId:      "",
	}
	reply := &AppendEntriesReply{}

	handleAppendEntries(server, args, reply)

	if reply.Success != true {
		t.Errorf("Expected success to be true. Got %v", reply.Success)
	}

	if server.commitIndex != 1 {
		t.Errorf("Expected commit index to be 1. Got %d", server.commitIndex)
	}

	server.logs.entries[2] = &LogEntry{Command: "x", Term: 1}
	server.logs.lastLogIndex = 2
	server.logs.lastLogTerm = 1
	args = &AppendEntriesArgs{
		Term:         2,
		LeaderId:     2,
		PrevLogIndex: 1,
		PrevLogTerm:  1,
		Entries:      []*LogEntry{},
		LeaderCommit: 3,
		TraceId:      "",
	}
	reply = &AppendEntriesReply{}

	handleAppendEntries(server, args, reply)

	if reply.Success != true {
		t.Errorf("Expected success to be true. Got %v", reply.Success)
	}

	if server.commitIndex != 2 {
		t.Errorf("Expected commit index to be 2. Got %d", server.commitIndex)
	}
}
