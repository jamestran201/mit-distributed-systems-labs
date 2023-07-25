package raft

import (
	"reflect"
	"testing"
)

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
		cfg := make_config(t, 1, false, false)
		defer cfg.cleanup()

		server := cfg.rafts[0]
		server.state = LEADER
		server.currentTerm = 2
		server.nextIndex = []int{0, 3}
		server.matchIndex = []int{0, 2}

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
