package raft

import "testing"

func TestResetsToFollowerWhenReplyHasHigherTerm(t *testing.T) {
	cfg := make_config(t, 1, false, false)
	defer cfg.cleanup()

	server := cfg.rafts[0]
	server.currentTerm = 1
	server.state = leader
	args := &AppendEntriesArgs{
		Term:         1,
		LeaderId:     0,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []*LogEntry{},
		LeaderCommit: 0,
		TraceId:      "",
	}
	reply := &AppendEntriesReply{Term: 2, Success: false, RequestCompleted: true, FirstConflictingIndex: 0}

	client := &AppendEntriesClient{server, 1, "", args, reply}
	result := client.handleAppendEntriesResponse()

	if server.state != follower {
		t.Errorf("Expected server to be in follower state but was in %d state", server.state)
	}

	if server.currentTerm != 2 {
		t.Errorf("Expected server current term to be 2 but was %d", server.currentTerm)
	}

	if result != terminate {
		t.Errorf("Expected result to be terminate but was %d", result)
	}
}

func TestUpdatesNextIndexWhenConflictingLogs(t *testing.T) {
	cfg := make_config(t, 2, false, false)
	defer cfg.cleanup()

	server := cfg.rafts[0]
	server.currentTerm = 1
	server.state = leader
	server.nextIndex[1] = 19
	args := &AppendEntriesArgs{
		Term:         1,
		LeaderId:     0,
		PrevLogIndex: 18,
		PrevLogTerm:  1,
		Entries:      []*LogEntry{},
		LeaderCommit: 0,
		TraceId:      "",
	}
	reply := &AppendEntriesReply{Term: 1, Success: false, RequestCompleted: true, FirstConflictingIndex: 7}

	client := &AppendEntriesClient{server, 1, "", args, reply}
	result := client.handleAppendEntriesResponse()

	if server.nextIndex[1] != 7 {
		t.Errorf("Expected nextIndex to be 7 but was %d", server.nextIndex[1])
	}

	if result != failure {
		t.Errorf("Expected result to be terminate but was %d", result)
	}
}

func TestUpdatesNextIndexAndMatchIndexWhenSuccess(t *testing.T) {
	cfg := make_config(t, 2, false, false)
	defer cfg.cleanup()

	server := cfg.rafts[0]
	server.currentTerm = 1
	server.state = leader
	server.nextIndex[1] = 19
	for i := 1; i <= 19; i++ {
		server.logs.entries[i] = &LogEntry{Term: 1, Command: "test"}
	}

	args := &AppendEntriesArgs{
		Term:         1,
		LeaderId:     0,
		PrevLogIndex: 18,
		PrevLogTerm:  1,
		Entries:      []*LogEntry{{Term: 1, Command: "test"}},
		LeaderCommit: 0,
		TraceId:      "",
	}
	reply := &AppendEntriesReply{Term: 1, Success: true, RequestCompleted: true, FirstConflictingIndex: 0}

	client := &AppendEntriesClient{server, 1, "", args, reply}
	result := client.handleAppendEntriesResponse()

	if server.nextIndex[1] != 20 {
		t.Errorf("Expected nextIndex to be 20 but was %d", server.nextIndex[1])
	}

	if server.matchIndex[1] != 19 {
		t.Errorf("Expected matchIndex to be 19 but was %d", server.matchIndex[1])
	}

	if result != success {
		t.Errorf("Expected result to be success but was %d", result)
	}
}

func TestDoesNotUpdateNextIndexAndMatchIndexWhenTheRequestIsForAnOlderLogEntry(t *testing.T) {
	cfg := make_config(t, 2, false, false)
	defer cfg.cleanup()

	server := cfg.rafts[0]
	server.currentTerm = 1
	server.state = leader
	server.nextIndex[1] = 19
	server.matchIndex[1] = 18
	for i := 1; i <= 19; i++ {
		server.logs.entries[i] = &LogEntry{Term: 1, Command: "test"}
	}

	args := &AppendEntriesArgs{
		Term:         1,
		LeaderId:     0,
		PrevLogIndex: 14,
		PrevLogTerm:  1,
		Entries:      []*LogEntry{{Term: 1, Command: "test"}, {Term: 1, Command: "test"}},
		LeaderCommit: 0,
		TraceId:      "",
	}
	reply := &AppendEntriesReply{Term: 1, Success: true, RequestCompleted: true, FirstConflictingIndex: 0}

	client := &AppendEntriesClient{server, 1, "", args, reply}
	result := client.handleAppendEntriesResponse()

	if server.nextIndex[1] != 19 {
		t.Errorf("Expected nextIndex to be 19 but was %d", server.nextIndex[1])
	}

	if server.matchIndex[1] != 18 {
		t.Errorf("Expected matchIndex to be 18 but was %d", server.matchIndex[1])
	}

	if result != terminate {
		t.Errorf("Expected result to be terminate but was %d", result)
	}
}

func TestUpdatesCommitIndexOnLeader(t *testing.T) {
	cfg := make_config(t, 2, false, false)
	defer cfg.cleanup()

	server := cfg.rafts[0]
	server.currentTerm = 1
	server.state = leader
	server.nextIndex[1] = 19
	server.matchIndex[1] = 18
	for i := 1; i <= 19; i++ {
		server.logs.entries[i] = &LogEntry{Term: 1, Command: "test"}
	}

	args := &AppendEntriesArgs{
		Term:         1,
		LeaderId:     0,
		PrevLogIndex: 18,
		PrevLogTerm:  1,
		Entries:      []*LogEntry{{Term: 1, Command: "test"}},
		LeaderCommit: 0,
		TraceId:      "",
	}
	reply := &AppendEntriesReply{Term: 1, Success: true, RequestCompleted: true, FirstConflictingIndex: 0}

	client := &AppendEntriesClient{server, 1, "", args, reply}
	client.handleAppendEntriesResponse()

	if server.commitIndex != 19 {
		t.Errorf("Expected commitIndex to be 19 but was %d", server.commitIndex)
	}
}
