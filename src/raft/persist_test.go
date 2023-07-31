package raft

import (
	"reflect"
	"testing"
)

func TestRaft_persisting_states(t *testing.T) {
	cfg := make_config(t, 1, false, false)
	defer cfg.cleanup()

	server := cfg.rafts[0]
	server.state = LEADER
	server.votedFor = 0
	server.currentTerm = 9
	server.lastLogIndex = 9
	server.lastLogTerm = 9
	server.logs = []LogEntry{
		{nil, 0},
		{"foo", 1},
		{"bar", 2},
		{"baz", 3},
		{"bap", 4},
		{"sar", 5},
		{"sap", 6},
		{"cra", 7},
		{"boo", 8},
		{"noo", 9},
	}

	server.persist()

	// Change the server's state to something random, so that we can verify that the persisted state is restored correctly
	server.state = CANDIDATE
	server.currentTerm = 100
	server.votedFor = 100
	server.lastLogIndex = 100
	server.lastLogTerm = 100
	server.logs = []LogEntry{
		{nil, 100},
		{"foo", 101},
		{"bar", 102},
	}

	server.readPersist(server.persister.ReadRaftState())

	expectedVotedFor := 0
	expectedCurrentTerm := 9
	expectedLogs := []LogEntry{
		{nil, 0},
		{"foo", 1},
		{"bar", 2},
		{"baz", 3},
		{"bap", 4},
		{"sar", 5},
		{"sap", 6},
		{"cra", 7},
		{"boo", 8},
		{"noo", 9},
	}
	expectedLastLogIndex := 9
	expectedLastLogTerm := 9
	if server.votedFor != expectedVotedFor {
		t.Errorf("Expected votedFor to be %d. Got %d", expectedVotedFor, server.votedFor)
	}

	if server.currentTerm != expectedCurrentTerm {
		t.Errorf("Expected currentTerm to be %d. Got %d", expectedCurrentTerm, server.currentTerm)
	}

	if !reflect.DeepEqual(server.logs, expectedLogs) {
		t.Errorf("Expected logs to be %+v. Got %+v", expectedLogs, server.logs)
	}

	if server.lastLogIndex != expectedLastLogIndex {
		t.Errorf("Expected lastLogIndex to be %d. Got %d", expectedLastLogIndex, server.lastLogIndex)
	}

	if server.lastLogTerm != expectedLastLogTerm {
		t.Errorf("Expected lastLogTerm to be %d. Got %d", expectedLastLogTerm, server.lastLogTerm)
	}
}
