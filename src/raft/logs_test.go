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

func TestRaft_findConflictIndexAndTerm(t *testing.T) {
	t.Run("When reason is no_logs_at_index, and server has some logs", func(t *testing.T) {
		rf := &Raft{
			currentTerm: 8,
			logs: []LogEntry{
				{Command: nil, Term: 0},
				{Command: "foo", Term: 2},
				{Command: "bar", Term: 2},
				{Command: "baz", Term: 5},
				{Command: "boo", Term: 5},
				{Command: "bee", Term: 7},
				{Command: "hoo", Term: 7},
				{Command: "zoo", Term: 7},
			},
			lastLogIndex: 7,
			lastLogTerm:  7,
		}

		index, term := rf.findConflictIndexAndTerm("no_logs_at_index", 8, 8)

		if index != 5 {
			t.Errorf("Expected index to be %d. Got %d", 5, index)
		}

		if term != 7 {
			t.Errorf("Expected term to be %d. Got %d", 7, term)
		}
	})

	t.Run("When reason is no_logs_at_index, and server has no logs", func(t *testing.T) {
		rf := &Raft{
			currentTerm: 8,
			logs:        []LogEntry{{Command: nil, Term: 0}},
		}
		rf.lastLogIndex = 0
		rf.lastLogTerm = 0

		index, term := rf.findConflictIndexAndTerm("no_logs_at_index", 8, 8)

		if index != 0 {
			t.Errorf("Expected index to be %d. Got %d", 0, index)
		}

		if term != 0 {
			t.Errorf("Expected term to be %d. Got %d", 0, term)
		}
	})

	t.Run("When reason is terms_do_not_match, and server has log at prevLogIndex", func(t *testing.T) {
		rf := &Raft{
			currentTerm: 8,
			logs: []LogEntry{
				{Command: nil, Term: 0},
				{Command: "foo", Term: 2},
				{Command: "bar", Term: 2},
				{Command: "baz", Term: 5},
				{Command: "boo", Term: 5},
				{Command: "bee", Term: 7},
				{Command: "hoo", Term: 7},
				{Command: "zoo", Term: 7},
			},
			lastLogIndex: 7,
			lastLogTerm:  7,
		}

		index, term := rf.findConflictIndexAndTerm("terms_do_not_match", 7, 8)

		if index != 5 {
			t.Errorf("Expected index to be %d. Got %d", 5, index)
		}

		if term != 7 {
			t.Errorf("Expected term to be %d. Got %d", 7, term)
		}
	})

	t.Run("When reason is terms_do_not_match, and server has more logs than prevLogIndex", func(t *testing.T) {
		rf := &Raft{
			currentTerm: 8,
			logs: []LogEntry{
				{Command: nil, Term: 0},
				{Command: "foo", Term: 2},
				{Command: "bar", Term: 2},
				{Command: "baz", Term: 5},
				{Command: "boo", Term: 5},
				{Command: "moo", Term: 5},
				{Command: "io", Term: 5},
				{Command: "bee", Term: 7},
				{Command: "hoo", Term: 7},
				{Command: "zoo", Term: 7},
				{Command: "loo", Term: 7},
				{Command: "croo", Term: 7},
				{Command: "too", Term: 7},
			},
			lastLogIndex: 12,
			lastLogTerm:  7,
		}

		index, term := rf.findConflictIndexAndTerm("terms_do_not_match", 9, 8)

		if index != 7 {
			t.Errorf("Expected index to be %d. Got %d", 7, index)
		}

		if term != 7 {
			t.Errorf("Expected term to be %d. Got %d", 7, term)
		}
	})
}

func Test_findFirstConflictIndex(t *testing.T) {
	t.Run("Returns -1 for both indices when server already contains the new entries", func(t *testing.T) {
		rf := &Raft{
			currentTerm: 8,
			logs: []LogEntry{
				{Command: nil, Term: 0},
				{Command: "foo", Term: 2},
				{Command: "bar", Term: 5},
				{Command: "baz", Term: 8},
			},
			lastLogIndex: 3,
			lastLogTerm:  8,
		}
		args := &AppendEntriesArgs{
			Term:         8,
			LeaderId:     1,
			PrevLogIndex: 1,
			PrevLogTerm:  2,
			Entries: []LogEntry{
				{Command: "bar", Term: 5},
				{Command: "baz", Term: 8},
			},
		}

		conflictIndex, firstNewIndex := rf.findFirstConflictIndex(args)

		if conflictIndex != -1 {
			t.Errorf("Expected conflictIndex to be -1. Got %d", conflictIndex)
		}

		if firstNewIndex != -1 {
			t.Errorf("Expected firstNewIndex to be -1. Got %d", firstNewIndex)
		}
	})

	t.Run("Returns -1 for both indices when the new entries are empty", func(t *testing.T) {
		rf := &Raft{
			currentTerm: 8,
			logs: []LogEntry{
				{Command: nil, Term: 0},
				{Command: "foo", Term: 2},
				{Command: "bar", Term: 5},
				{Command: "baz", Term: 8},
			},
			lastLogIndex: 3,
			lastLogTerm:  8,
		}
		args := &AppendEntriesArgs{
			Term:         8,
			LeaderId:     1,
			PrevLogIndex: 3,
			PrevLogTerm:  8,
			Entries:      []LogEntry{},
		}

		conflictIndex, firstNewIndex := rf.findFirstConflictIndex(args)

		if conflictIndex != -1 {
			t.Errorf("Expected conflictIndex to be -1. Got %d", conflictIndex)
		}

		if firstNewIndex != -1 {
			t.Errorf("Expected firstNewIndex to be -1. Got %d", firstNewIndex)
		}
	})

	t.Run("Does not account for the 0th log entry", func(t *testing.T) {
		rf := &Raft{
			currentTerm: 1,
			logs: []LogEntry{
				{Command: nil, Term: 0},
			},
			lastLogIndex: 0,
			lastLogTerm:  0,
		}
		args := &AppendEntriesArgs{
			Term:         1,
			LeaderId:     1,
			PrevLogIndex: 0,
			PrevLogTerm:  0,
			Entries:      []LogEntry{{Command: "foo", Term: 1}},
		}

		conflictIndex, firstNewIndex := rf.findFirstConflictIndex(args)

		if conflictIndex != -1 {
			t.Errorf("Expected conflictIndex to be -1. Got %d", conflictIndex)
		}

		if firstNewIndex != 0 {
			t.Errorf("Expected firstNewIndex to be 0. Got %d", firstNewIndex)
		}
	})

	t.Run("Returns first conflict index and first new index when server contains a prefix of the new entries", func(t *testing.T) {
		rf := &Raft{
			currentTerm: 8,
			logs: []LogEntry{
				{Command: nil, Term: 0},
				{Command: "foo", Term: 2},
				{Command: "croc", Term: 3},
				{Command: "bar", Term: 5},
				{Command: "cos", Term: 6},
				{Command: "baz", Term: 8},
			},
			lastLogIndex: 5,
			lastLogTerm:  8,
		}
		args := &AppendEntriesArgs{
			Term:         9,
			LeaderId:     1,
			PrevLogIndex: 1,
			PrevLogTerm:  2,
			Entries: []LogEntry{
				{Command: "croc", Term: 3},
				{Command: "bar", Term: 5},
				{Command: "wut", Term: 7},
				{Command: "cool", Term: 8},
				{Command: "bean", Term: 9},
			},
		}

		conflictIndex, firstNewIndex := rf.findFirstConflictIndex(args)

		expectedConflictIndex := 4
		expectedFirstNewIndex := 2
		if conflictIndex != expectedConflictIndex {
			t.Errorf("Expected conflictIndex to be %d. Got %d", expectedConflictIndex, conflictIndex)
		}

		if firstNewIndex != expectedFirstNewIndex {
			t.Errorf("Expected firstNewIndex to be %d. Got %d", expectedFirstNewIndex, firstNewIndex)
		}
	})

	t.Run("Returns first conflict index and first new index when no logs match after prevLogIndex match", func(t *testing.T) {
		rf := &Raft{
			currentTerm: 8,
			logs: []LogEntry{
				{Command: nil, Term: 0},
				{Command: "foo", Term: 2},
				{Command: "croc", Term: 2},
				{Command: "bar", Term: 2},
				{Command: "cos", Term: 2},
				{Command: "baz", Term: 2},
			},
			lastLogIndex: 5,
			lastLogTerm:  2,
		}
		args := &AppendEntriesArgs{
			Term:         9,
			LeaderId:     1,
			PrevLogIndex: 1,
			PrevLogTerm:  2,
			Entries: []LogEntry{
				{Command: "croc", Term: 3},
				{Command: "bar", Term: 5},
				{Command: "wut", Term: 7},
				{Command: "cool", Term: 8},
				{Command: "bean", Term: 9},
			},
		}

		conflictIndex, firstNewIndex := rf.findFirstConflictIndex(args)

		expectedConflictIndex := 2
		expectedFirstNewIndex := 0
		if conflictIndex != expectedConflictIndex {
			t.Errorf("Expected conflictIndex to be %d. Got %d", expectedConflictIndex, conflictIndex)
		}

		if firstNewIndex != expectedFirstNewIndex {
			t.Errorf("Expected firstNewIndex to be %d. Got %d", expectedFirstNewIndex, firstNewIndex)
		}
	})

	t.Run("Returns the index of the first new entry when there is no conflict", func(t *testing.T) {
		rf := &Raft{
			currentTerm: 8,
			logs: []LogEntry{
				{Command: nil, Term: 0},
				{Command: "foo", Term: 2},
				{Command: "bar", Term: 5},
				{Command: "baz", Term: 8},
			},
			lastLogIndex: 3,
			lastLogTerm:  8,
		}
		args := &AppendEntriesArgs{
			Term:         9,
			LeaderId:     1,
			PrevLogIndex: 3,
			PrevLogTerm:  8,
			Entries: []LogEntry{
				{Command: "qux", Term: 8},
				{Command: "sun", Term: 9},
			},
		}

		conflictIndex, firstNewIndex := rf.findFirstConflictIndex(args)

		expectedConflictIndex := -1
		expectedFirstNewIndex := 0
		if conflictIndex != expectedConflictIndex {
			t.Errorf("Expected conflictIndex to be %d. Got %d", expectedConflictIndex, conflictIndex)
		}

		if firstNewIndex != expectedFirstNewIndex {
			t.Errorf("Expected firstNewIndex to be %d. Got %d", expectedFirstNewIndex, firstNewIndex)
		}
	})

	t.Run("Returns first new index when server contains a prefix of new entries", func(t *testing.T) {
		rf := &Raft{
			currentTerm: 8,
			logs: []LogEntry{
				{Command: nil, Term: 0},
				{Command: "foo", Term: 2},
				{Command: "bar", Term: 5},
				{Command: "baz", Term: 8},
			},
			lastLogIndex: 3,
			lastLogTerm:  8,
		}
		args := &AppendEntriesArgs{
			Term:         9,
			LeaderId:     1,
			PrevLogIndex: 1,
			PrevLogTerm:  2,
			Entries: []LogEntry{
				{Command: "bar", Term: 5},
				{Command: "baz", Term: 8},
				{Command: "qux", Term: 8},
				{Command: "sun", Term: 9},
			},
		}

		conflictIndex, firstNewIndex := rf.findFirstConflictIndex(args)

		expectedConflictIndex := -1
		expectedFirstNewIndex := 2
		if conflictIndex != expectedConflictIndex {
			t.Errorf("Expected conflictIndex to be %d. Got %d", expectedConflictIndex, conflictIndex)
		}

		if firstNewIndex != expectedFirstNewIndex {
			t.Errorf("Expected firstNewIndex to be %d. Got %d", expectedFirstNewIndex, firstNewIndex)
		}
	})
}

func Test_deleteAllLogsFrom(t *testing.T) {
	t.Run("Deletes all logs starting at given index", func(t *testing.T) {
		rf := &Raft{
			currentTerm: 8,
			logs: []LogEntry{
				{Command: nil, Term: 0},
				{Command: "foo", Term: 2},
				{Command: "boo", Term: 3},
				{Command: "loo", Term: 4},
				{Command: "bar", Term: 5},
				{Command: "baz", Term: 8},
			},
			lastLogIndex: 5,
			lastLogTerm:  8,
		}

		err := rf.deleteAllLogsFrom(3)
		if err != nil {
			t.Errorf("Expected no error. Got %v", err)
		}

		expectedLogs := []LogEntry{
			{Command: nil, Term: 0},
			{Command: "foo", Term: 2},
			{Command: "boo", Term: 3},
		}
		expectedLastLogIndex := 2
		expectedLastLogTerm := 3
		if !reflect.DeepEqual(rf.logs, expectedLogs) {
			t.Errorf("Expected logs to be %+v. Got %+v", expectedLogs, rf.logs)
		}

		if rf.lastLogIndex != expectedLastLogIndex {
			t.Errorf("Expected last log index to be %d. Got %d", expectedLastLogIndex, rf.lastLogIndex)
		}

		if rf.lastLogTerm != expectedLastLogTerm {
			t.Errorf("Expected last log term to be %d. Got %d", expectedLastLogTerm, rf.lastLogTerm)
		}
	})

	t.Run("Returns an error when given index is less than or equal to commitIndex", func(t *testing.T) {
		rf := &Raft{
			currentTerm: 8,
			logs: []LogEntry{
				{Command: nil, Term: 0},
				{Command: "foo", Term: 2},
				{Command: "boo", Term: 3},
				{Command: "loo", Term: 4},
				{Command: "bar", Term: 5},
				{Command: "baz", Term: 8},
			},
			lastLogIndex: 5,
			lastLogTerm:  8,
			commitIndex:  3,
		}

		err := rf.deleteAllLogsFrom(3)
		if err == nil {
			t.Errorf("Expected an error. Got nil")
		}
	})
}

func Test_appendNewEntries(t *testing.T) {
	t.Run("Appends new entries to log", func(t *testing.T) {
		rf := &Raft{
			currentTerm: 3,
			logs: []LogEntry{
				{Command: nil, Term: 0},
				{Command: "foo", Term: 2},
				{Command: "boo", Term: 3},
			},
			lastLogIndex: 2,
			lastLogTerm:  3,
		}
		args := &AppendEntriesArgs{
			Term: 8,
			Entries: []LogEntry{
				{Command: "loo", Term: 4},
				{Command: "bar", Term: 5},
				{Command: "baz", Term: 8},
			},
		}

		rf.appendNewEntries(args, 0)

		expectedLogs := []LogEntry{
			{Command: nil, Term: 0},
			{Command: "foo", Term: 2},
			{Command: "boo", Term: 3},
			{Command: "loo", Term: 4},
			{Command: "bar", Term: 5},
			{Command: "baz", Term: 8},
		}
		if !reflect.DeepEqual(rf.logs, expectedLogs) {
			t.Errorf("Expected logs to be %+v. Got %+v", expectedLogs, rf.logs)
		}

		expectedLastLogIndex := 5
		expectedLastLogTerm := 8
		if rf.lastLogIndex != expectedLastLogIndex {
			t.Errorf("Expected last log index to be %d. Got %d", expectedLastLogIndex, rf.lastLogIndex)
		}

		if rf.lastLogTerm != expectedLastLogTerm {
			t.Errorf("Expected last log term to be %d. Got %d", expectedLastLogTerm, rf.lastLogTerm)
		}
	})
}
