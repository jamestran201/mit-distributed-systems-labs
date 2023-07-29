package raft

import (
	"reflect"
	"testing"
)

func TestRaft_makeAppendEntriesArgs(t *testing.T) {
	t.Run("Returns correct args when follower is up-to-date", func(t *testing.T) {
		cfg := make_config(t, 1, false, false)
		defer cfg.cleanup()

		server := cfg.rafts[0]
		server.state = LEADER
		server.currentTerm = 1
		server.lastLogIndex = 2
		server.lastLogTerm = 1
		server.logs = []LogEntry{
			{nil, 0},
			{"foo", 1},
			{"bar", 1},
		}
		server.nextIndex = []int{0, 3}
		server.commitIndex = 0

		args := server.makeAppendEntriesArgs(1, "abc")

		expectedArgs := &AppendEntriesArgs{
			Term:         1,
			LeaderId:     0,
			PrevLogIndex: 2,
			PrevLogTerm:  1,
			Entries:      []LogEntry{},
			LeaderCommit: 0,
			TraceId:      "abc",
		}
		if !reflect.DeepEqual(args, expectedArgs) {
			t.Errorf("Expected args to be %+v. Got %+v", expectedArgs, args)
		}
	})

	t.Run("Returns correct args when new logs need to be sent to follower", func(t *testing.T) {
		cfg := make_config(t, 1, false, false)
		defer cfg.cleanup()

		server := cfg.rafts[0]
		server.state = LEADER
		server.currentTerm = 2
		server.lastLogIndex = 3
		server.lastLogTerm = 2
		server.logs = []LogEntry{
			{nil, 0},
			{"foo", 1},
			{"bar", 1},
			{"baz", 2},
		}
		server.nextIndex = []int{0, 3}
		server.commitIndex = 0

		args := server.makeAppendEntriesArgs(1, "abc")

		expectedArgs := &AppendEntriesArgs{
			Term:         2,
			LeaderId:     0,
			PrevLogIndex: 2,
			PrevLogTerm:  1,
			Entries:      []LogEntry{{"baz", 2}},
			LeaderCommit: 0,
			TraceId:      "abc",
		}
		if !reflect.DeepEqual(args, expectedArgs) {
			t.Errorf("Expected args to be %+v. Got %+v", expectedArgs, args)
		}
	})
}

func TestRaft_handleAppendEntriesResponse(t *testing.T) {
	t.Run("Aborts when server state has changed from when the request was made", func(t *testing.T) {
		cfg := make_config(t, 1, false, false)
		defer cfg.cleanup()

		server := cfg.rafts[0]
		server.state = FOLLOWER
		server.lastLogIndex = 2
		server.lastLogTerm = 1
		server.logs = []LogEntry{
			{nil, 0},
			{"foo", 1},
			{"bar", 1},
		}

		server.handleAppendEntriesResponse(1, &AppendEntriesArgs{}, &AppendEntriesReply{})

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
			t.Errorf("Expected lastLogIndex to be %d. Got %d", expectedLastLogIndex, server.lastLogIndex)
		}

		expectedLastLogTerm := 1
		if server.lastLogTerm != expectedLastLogTerm {
			t.Errorf("Expected lastLogTerm to be %d. Got %d", expectedLastLogTerm, server.lastLogTerm)
		}
	})

	t.Run("Resets to follower when given term is higher", func(t *testing.T) {
		cfg := make_config(t, 1, false, false)
		defer cfg.cleanup()

		server := cfg.rafts[0]
		server.state = LEADER
		server.currentTerm = 2

		reply := &AppendEntriesReply{
			Term:    4,
			Success: false,
		}
		server.handleAppendEntriesResponse(1, &AppendEntriesArgs{}, reply)

		expectedState := FOLLOWER
		expectedCurrentTerm := 4
		if server.state != expectedState {
			t.Errorf("Expected state to be %s. Got %s", expectedState, server.state)
		}

		if server.currentTerm != expectedCurrentTerm {
			t.Errorf("Expected current term to be %d. Got %d", expectedCurrentTerm, server.currentTerm)
		}
	})

	t.Run("Decrements nextIndex and retries when result is not successful", func(t *testing.T) {
		cfg := make_config(t, 1, false, false)
		defer cfg.cleanup()

		server := cfg.rafts[0]
		server.state = LEADER
		server.currentTerm = 2
		server.nextIndex = []int{0, 3}

		args := &AppendEntriesArgs{
			Term:         2,
			LeaderId:     0,
			PrevLogIndex: 2,
			PrevLogTerm:  1,
			Entries:      []LogEntry{},
			LeaderCommit: 0,
		}
		reply := &AppendEntriesReply{
			Term:    2,
			Success: false,
		}
		server.handleAppendEntriesResponse(1, args, reply)

		expectedNextIndex := 2
		if server.nextIndex[1] != expectedNextIndex {
			t.Errorf("Expected nextIndex to be %d. Got %d", expectedNextIndex, server.nextIndex[1])
		}
	})

	t.Run("Increments nextIndex and matchIndex when result is successful", func(t *testing.T) {
		cfg := make_config(t, 3, false, false)
		defer cfg.cleanup()

		server := cfg.rafts[0]
		server.state = LEADER
		server.currentTerm = 2
		server.nextIndex = []int{0, 3, 2}
		server.matchIndex = []int{0, 2, 1}
		server.logs = []LogEntry{
			{nil, 0},
			{"blah", 1},
			{"grr", 1},
			{"foo", 2},
			{"bar", 2},
			{"baz", 2},
		}
		server.lastLogIndex = 5
		server.lastLogTerm = 2

		args := &AppendEntriesArgs{
			Term:         2,
			LeaderId:     0,
			PrevLogIndex: 2,
			PrevLogTerm:  1,
			Entries: []LogEntry{
				{"foo", 2},
				{"bar", 2},
				{"baz", 2},
			},
			LeaderCommit: 0,
		}
		reply := &AppendEntriesReply{
			Term:    2,
			Success: true,
		}
		server.handleAppendEntriesResponse(1, args, reply)

		expectedNextIndex := 6
		if server.nextIndex[1] != expectedNextIndex {
			t.Errorf("Expected nextIndex to be %d. Got %d", expectedNextIndex, server.nextIndex[1])
		}

		expectedMatchIndex := 5
		if server.matchIndex[1] != expectedMatchIndex {
			t.Errorf("Expected matchIndex to be %d. Got %d", expectedMatchIndex, server.matchIndex[1])
		}
	})
}

func Test_updateCommitIndexForLeader(t *testing.T) {
	cfg := make_config(t, 5, false, false)
	defer cfg.cleanup()

	server := cfg.rafts[0]
	server.state = LEADER
	server.currentTerm = 5
	server.commitIndex = 2
	server.lastLogIndex = 9
	server.lastLogTerm = 5
	server.matchIndex = []int{0, 8, 1, 2, 8}
	server.logs = []LogEntry{
		{nil, 0},
		{"foo", 1},
		{"bar", 2},
		{"baz", 2},
		{"qux", 3},
		{"quux", 4},
		{"corge", 4},
		{"grault", 5},
		{"garply", 5},
		{"burly", 5},
	}

	t.Run("no-ops when index is not higher than commitIndex", func(t *testing.T) {
		server.updateCommitIndexForLeader(1, "")

		expectedCommitIndex := 2
		if server.commitIndex != expectedCommitIndex {
			t.Errorf("Expected commitIndex to be %d. Got %d", expectedCommitIndex, server.commitIndex)
		}
	})

	t.Run("no-ops when log at index is not from the current term", func(t *testing.T) {
		server.updateCommitIndexForLeader(5, "")

		expectedCommitIndex := 2
		if server.commitIndex != expectedCommitIndex {
			t.Errorf("Expected commitIndex to be %d. Got %d", expectedCommitIndex, server.commitIndex)
		}
	})

	t.Run("no-ops when log at index is not replicated by a majority of servers", func(t *testing.T) {
		server.updateCommitIndexForLeader(9, "")

		expectedCommitIndex := 2
		if server.commitIndex != expectedCommitIndex {
			t.Errorf("Expected commitIndex to be %d. Got %d", expectedCommitIndex, server.commitIndex)
		}
	})

	t.Run("updates commitIndex when log at index is replicated by a majority of servers", func(t *testing.T) {
		server.updateCommitIndexForLeader(8, "")

		expectedCommitIndex := 8
		if server.commitIndex != expectedCommitIndex {
			t.Errorf("Expected commitIndex to be %d. Got %d", expectedCommitIndex, server.commitIndex)
		}
	})
}
