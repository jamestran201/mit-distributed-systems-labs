package raft

import (
	"reflect"
	"testing"
)

func Test_appendLogEntry(t *testing.T) {
	t.Run("Append entry to empty log", func(t *testing.T) {
		rf := &Raft{
			currentTerm:  2,
			logs:         []LogEntry{{Command: nil, Term: 0}},
			lastLogIndex: 0,
			lastLogTerm:  0,
		}

		res := rf.appendLogEntry("foo")

		expectedLogs := []LogEntry{
			{Command: nil, Term: 0},
			{Command: "foo", Term: 2},
		}
		expectedLastLogIndex := 1
		expectedLastLogTerm := 2
		if !reflect.DeepEqual(rf.logs, expectedLogs) {
			t.Errorf("Expected logs to be %+v. Got %+v", expectedLogs, rf.logs)
		}

		if rf.lastLogIndex != expectedLastLogIndex {
			t.Errorf("Expected last log index to be %d. Got %d", expectedLastLogIndex, rf.lastLogIndex)
		}

		if rf.lastLogTerm != expectedLastLogTerm {
			t.Errorf("Expected last log term to be %d. Got %d", expectedLastLogTerm, rf.lastLogTerm)
		}

		if res != expectedLastLogIndex {
			t.Errorf("Expected result to be %d. Got %d", expectedLastLogIndex, res)
		}
	})

	t.Run("Append entry to non-empty log", func(t *testing.T) {
		rf := &Raft{
			currentTerm: 11,
			logs: []LogEntry{
				{Command: nil, Term: 0},
				{Command: "foo", Term: 2},
				{Command: "bar", Term: 2},
				{Command: "baz", Term: 5},
				{Command: "bee", Term: 7},
			},
			lastLogIndex: 4,
			lastLogTerm:  7,
		}

		res := rf.appendLogEntry("qux")

		expectedLogs := []LogEntry{
			{Command: nil, Term: 0},
			{Command: "foo", Term: 2},
			{Command: "bar", Term: 2},
			{Command: "baz", Term: 5},
			{Command: "bee", Term: 7},
			{Command: "qux", Term: 11},
		}
		expectedLastLogIndex := 5
		expectedLastLogTerm := 11
		if !reflect.DeepEqual(rf.logs, expectedLogs) {
			t.Errorf("Expected logs to be %+v. Got %+v", expectedLogs, rf.logs)
		}

		if rf.lastLogIndex != expectedLastLogIndex {
			t.Errorf("Expected last log index to be %d. Got %d", expectedLastLogIndex, rf.lastLogIndex)
		}

		if rf.lastLogTerm != expectedLastLogTerm {
			t.Errorf("Expected last log term to be %d. Got %d", expectedLastLogTerm, rf.lastLogTerm)
		}

		if res != expectedLastLogIndex {
			t.Errorf("Expected result to be %d. Got %d", expectedLastLogIndex, res)
		}
	})
}

func Test_hasLogWithIndexAndTerm(t *testing.T) {
	rf := &Raft{
		currentTerm: 11,
		logs: []LogEntry{
			{Command: nil, Term: 0},
			{Command: "foo", Term: 2},
			{Command: "bar", Term: 2},
			{Command: "baz", Term: 5},
			{Command: "bee", Term: 7},
		},
		lastLogIndex: 4,
		lastLogTerm:  7,
	}

	t.Run("Returns false when given index is greater than server lastLogIndex", func(t *testing.T) {
		res, _ := rf.hasLogWithIndexAndTerm(5, 7)

		if res {
			t.Errorf("Expected result to be false. Got true")
		}
	})

	t.Run("Returns false when given index is less than 0", func(t *testing.T) {
		res, _ := rf.hasLogWithIndexAndTerm(-1, 7)

		if res {
			t.Errorf("Expected result to be false. Got true")
		}
	})

	t.Run("Returns false when server has log at given index but term does not match", func(t *testing.T) {
		res, _ := rf.hasLogWithIndexAndTerm(4, 13)

		if res {
			t.Errorf("Expected result to be false. Got true")
		}
	})

	t.Run("Returns true when server has log at given index and term matches", func(t *testing.T) {
		res, _ := rf.hasLogWithIndexAndTerm(4, 7)
		if !res {
			t.Errorf("Expected result to be true. Got false")
		}

		res, _ = rf.hasLogWithIndexAndTerm(2, 2)
		if !res {
			t.Errorf("Expected result to be true. Got false")
		}

		res, _ = rf.hasLogWithIndexAndTerm(0, 0)
		if !res {
			t.Errorf("Expected result to be true. Got false")
		}
	})
}
