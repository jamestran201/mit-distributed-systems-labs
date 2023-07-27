package raft

import (
	"reflect"
	"testing"
)

func TestRejection(t *testing.T) {
	t.Run("Rejects when given term is less than current term", func(t *testing.T) {
		cfg := make_config(t, 1, false, false)
		defer cfg.cleanup()

		server := cfg.rafts[0]
		server.currentTerm = 2

		args := &AppendEntriesArgs{
			Term:     1,
			LeaderId: 1,
		}
		reply := &AppendEntriesReply{}

		server.handleAppendEntries(args, reply)

		expectedTerm := 2
		expectedSuccess := false
		if reply.Term != expectedTerm {
			t.Errorf("Expected term to be %d. Got %d", expectedTerm, reply.Term)
		}

		if reply.Success != expectedSuccess {
			t.Errorf("Expected success to be %v. Got %v", expectedSuccess, reply.Success)
		}
	})

	t.Run("Rejects when server has no logs at prevLogIndex", func(t *testing.T) {
		cfg := make_config(t, 1, false, false)
		defer cfg.cleanup()

		server := cfg.rafts[0]
		server.currentTerm = 2
		server.logs = []LogEntry{
			{nil, 0},
			{"foo", 2},
		}
		server.lastLogIndex = 1
		server.lastLogTerm = 2

		args := &AppendEntriesArgs{
			Term:         2,
			LeaderId:     1,
			PrevLogIndex: 4,
			PrevLogTerm:  2,
		}
		reply := &AppendEntriesReply{}

		server.handleAppendEntries(args, reply)

		expectedSuccess := false
		if reply.Success != expectedSuccess {
			t.Errorf("Expected success to be %v. Got %v", expectedSuccess, reply.Success)
		}
	})

	t.Run("Rejects when terms do not match", func(t *testing.T) {
		cfg := make_config(t, 1, false, false)
		defer cfg.cleanup()

		server := cfg.rafts[0]
		server.currentTerm = 2
		server.logs = []LogEntry{
			{nil, 0},
			{"foo", 2},
		}
		server.lastLogIndex = 1
		server.lastLogTerm = 2

		args := &AppendEntriesArgs{
			Term:         2,
			LeaderId:     1,
			PrevLogIndex: 1,
			PrevLogTerm:  8,
		}
		reply := &AppendEntriesReply{}

		server.handleAppendEntries(args, reply)

		expectedSuccess := false
		if reply.Success != expectedSuccess {
			t.Errorf("Expected success to be %v. Got %v", expectedSuccess, reply.Success)
		}
	})
}

func TestResetsToFollowerWhenHandlingAppendEntries(t *testing.T) {
	cfg := make_config(t, 1, false, false)
	defer cfg.cleanup()

	server := cfg.rafts[0]
	server.currentTerm = 2
	server.state = LEADER

	// Test when given term is greater than current term
	args := &AppendEntriesArgs{
		Term:     4,
		LeaderId: 1,
	}
	reply := &AppendEntriesReply{}

	server.handleAppendEntries(args, reply)

	expectedTerm := 4
	expectedState := FOLLOWER
	if server.currentTerm != expectedTerm {
		t.Errorf("Expected current term to be %d. Got %d", expectedTerm, server.currentTerm)
	}

	if server.state != expectedState {
		t.Errorf("Expected state to be %s. Got %s", expectedState, server.state)
	}

	// Test when candidate receives AppendEntries from leader
	server.currentTerm = 4
	server.state = CANDIDATE

	server.handleAppendEntries(args, reply)

	expectedTerm = 4
	expectedState = FOLLOWER
	if server.currentTerm != expectedTerm {
		t.Errorf("Expected current term to be %d. Got %d", expectedTerm, server.currentTerm)
	}

	if server.state != expectedState {
		t.Errorf("Expected state to be %s. Got %s", expectedState, server.state)
	}
}

func TestSetsReceivedRpcFromPeerToTrueWhenAppendEntriesIsValid(t *testing.T) {
	cfg := make_config(t, 1, false, false)
	defer cfg.cleanup()

	server := cfg.rafts[0]
	server.currentTerm = 2
	server.state = FOLLOWER
	server.receivedRpcFromPeer = false

	args := &AppendEntriesArgs{
		Term:     2,
		LeaderId: 1,
	}
	reply := &AppendEntriesReply{}

	server.handleAppendEntries(args, reply)

	expectedReceivedRpcFromPeer := true
	if server.receivedRpcFromPeer != expectedReceivedRpcFromPeer {
		t.Errorf("Expected received rpc from peer to be %v. Got %v", expectedReceivedRpcFromPeer, server.receivedRpcFromPeer)
	}
}

func TestDoesNothingWhenCurrentLogsAreUpToDate(t *testing.T) {
	cfg := make_config(t, 1, false, false)
	defer cfg.cleanup()

	server := cfg.rafts[0]
	server.currentTerm = 1
	server.state = FOLLOWER
	server.receivedRpcFromPeer = false
	server.logs = []LogEntry{
		{nil, 0},
		{"foo", 1},
		{"bar", 1},
	}
	server.lastLogIndex = 2
	server.lastLogTerm = 1

	args := &AppendEntriesArgs{
		Term:         2,
		LeaderId:     1,
		PrevLogIndex: 2,
		PrevLogTerm:  1,
		Entries:      []LogEntry{},
		LeaderCommit: 0,
	}
	reply := &AppendEntriesReply{}

	server.handleAppendEntries(args, reply)

	expectedLogs := []LogEntry{
		{nil, 0},
		{"foo", 1},
		{"bar", 1},
	}
	if !reflect.DeepEqual(server.logs, expectedLogs) {
		t.Errorf("Expected logs to be %+v. Got %+v", expectedLogs, server.logs)
	}

	expectedLastLogIndex := 2
	if server.lastLogIndex != expectedLastLogIndex {
		t.Errorf("Expected last log index to be %d. Got %d", expectedLastLogIndex, server.lastLogIndex)
	}

	expectedLastLogTerm := 1
	if server.lastLogTerm != expectedLastLogTerm {
		t.Errorf("Expected last log term to be %d. Got %d", expectedLastLogTerm, server.lastLogTerm)
	}

	if reply.Success != true {
		t.Errorf("Expected success to be true. Got %v", reply.Success)
	}
}

func TestDeletesConflictingLogsAndAppendsNewLogs(t *testing.T) {
	cfg := make_config(t, 1, false, false)
	defer cfg.cleanup()

	server := cfg.rafts[0]
	server.currentTerm = 3
	server.state = FOLLOWER
	server.receivedRpcFromPeer = false
	server.logs = []LogEntry{
		{nil, 0},
		{"foo", 1},
		{"bar", 1},
		{"qar", 2},
		{"qoo", 3},
		{"zoo", 3},
	}
	server.lastLogIndex = 5
	server.lastLogTerm = 3

	args := &AppendEntriesArgs{
		Term:         4,
		LeaderId:     1,
		PrevLogIndex: 2,
		PrevLogTerm:  1,
		Entries: []LogEntry{
			{"moo", 3},
			{"shoo", 3},
			{"loo", 4},
			{"joo", 4},
		},
		LeaderCommit: 0,
	}
	reply := &AppendEntriesReply{}

	server.handleAppendEntries(args, reply)

	expectedLogs := []LogEntry{
		{nil, 0},
		{"foo", 1},
		{"bar", 1},
		{"moo", 3},
		{"shoo", 3},
		{"loo", 4},
		{"joo", 4},
	}
	if !reflect.DeepEqual(server.logs, expectedLogs) {
		t.Errorf("Expected logs to be %+v. Got %+v", expectedLogs, server.logs)
	}

	expectedLastLogIndex := 6
	if server.lastLogIndex != expectedLastLogIndex {
		t.Errorf("Expected last log index to be %d. Got %d", expectedLastLogIndex, server.lastLogIndex)
	}

	expectedLastLogTerm := 4
	if server.lastLogTerm != expectedLastLogTerm {
		t.Errorf("Expected last log term to be %d. Got %d", expectedLastLogTerm, server.lastLogTerm)
	}

	if reply.Success != true {
		t.Errorf("Expected success to be true. Got %v", reply.Success)
	}
}

func TestOnlyAppendNewLogs(t *testing.T) {
	cfg := make_config(t, 1, false, false)
	defer cfg.cleanup()

	server := cfg.rafts[0]
	server.currentTerm = 1
	server.state = FOLLOWER
	server.receivedRpcFromPeer = false
	server.logs = []LogEntry{
		{nil, 0},
	}
	server.lastLogIndex = 0
	server.lastLogTerm = 0

	args := &AppendEntriesArgs{
		Term:         1,
		LeaderId:     1,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries: []LogEntry{
			{"moo", 1},
			{"shoo", 1},
			{"loo", 1},
			{"joo", 1},
		},
		LeaderCommit: 0,
	}
	reply := &AppendEntriesReply{}

	server.handleAppendEntries(args, reply)

	expectedLogs := []LogEntry{
		{nil, 0},
		{"moo", 1},
		{"shoo", 1},
		{"loo", 1},
		{"joo", 1},
	}
	if !reflect.DeepEqual(server.logs, expectedLogs) {
		t.Errorf("Expected logs to be %+v. Got %+v", expectedLogs, server.logs)
	}

	expectedLastLogIndex := 4
	if server.lastLogIndex != expectedLastLogIndex {
		t.Errorf("Expected last log index to be %d. Got %d", expectedLastLogIndex, server.lastLogIndex)
	}

	expectedLastLogTerm := 1
	if server.lastLogTerm != expectedLastLogTerm {
		t.Errorf("Expected last log term to be %d. Got %d", expectedLastLogTerm, server.lastLogTerm)
	}

	if reply.Success != true {
		t.Errorf("Expected success to be true. Got %v", reply.Success)
	}
}

func TestUpdateCommitIndexWhenHandlingAppendEntries(t *testing.T) {
	t.Run("Sets commit index to leader commit when leader commit is less than lastLogIndex", func(t *testing.T) {
		cfg := make_config(t, 1, false, false)
		defer cfg.cleanup()

		server := cfg.rafts[0]
		server.currentTerm = 1
		server.state = FOLLOWER
		server.receivedRpcFromPeer = false
		server.logs = []LogEntry{
			{nil, 0},
		}
		server.lastLogIndex = 0
		server.lastLogTerm = 0
		server.commitIndex = 0

		args := &AppendEntriesArgs{
			Term:         1,
			LeaderId:     1,
			PrevLogIndex: 0,
			PrevLogTerm:  0,
			Entries: []LogEntry{
				{"moo", 1},
				{"shoo", 1},
				{"loo", 1},
				{"joo", 1},
			},
			LeaderCommit: 2,
		}
		reply := &AppendEntriesReply{}

		server.handleAppendEntries(args, reply)

		expectedCommitIndex := 2
		if server.commitIndex != expectedCommitIndex {
			t.Errorf("Expected commit index to be %d. Got %d", expectedCommitIndex, server.commitIndex)
		}
	})

	t.Run("Sets commit index to lastLogIndex when leaderCommit is higher than lastLogIndex", func(t *testing.T) {
		cfg := make_config(t, 1, false, false)
		defer cfg.cleanup()

		server := cfg.rafts[0]
		server.currentTerm = 1
		server.state = FOLLOWER
		server.receivedRpcFromPeer = false
		server.logs = []LogEntry{
			{nil, 0},
			{"moo", 1},
			{"shoo", 1},
			{"loo", 1},
		}
		server.lastLogIndex = 3
		server.lastLogTerm = 1
		server.commitIndex = 0

		args := &AppendEntriesArgs{
			Term:         1,
			LeaderId:     1,
			PrevLogIndex: 3,
			PrevLogTerm:  1,
			Entries:      []LogEntry{},
			LeaderCommit: 4,
		}
		reply := &AppendEntriesReply{}

		server.handleAppendEntries(args, reply)

		expectedCommitIndex := 3
		if server.commitIndex != expectedCommitIndex {
			t.Errorf("Expected commit index to be %d. Got %d", expectedCommitIndex, server.commitIndex)
		}
	})
}
