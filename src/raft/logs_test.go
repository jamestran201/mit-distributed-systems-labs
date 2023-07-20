package raft

import (
	"reflect"
	"testing"
)

func TestFirstIndexOfTerm(t *testing.T) {
	l := &Logs{
		entries:      map[int]*LogEntry{0: {Term: 0}, 1: {Term: 1}, 2: {Term: 1}, 3: {Term: 2}, 4: {Term: 3}, 5: {Term: 3}},
		lastLogIndex: 5,
		lastLogTerm:  3,
	}

	t.Run("Returns the first index of the given term", func(t *testing.T) {
		want := 3
		if got := l.firstIndexOfTerm(2); got != want {
			t.Errorf("Logs.firstIndexOfTerm() = %v, want %v", got, want)
		}
	})

	t.Run("Returns -1 when no index is found", func(t *testing.T) {
		want := -1
		if got := l.firstIndexOfTerm(16); got != want {
			t.Errorf("Logs.firstIndexOfTerm() = %v, want %v", got, want)
		}
	})
}

func TestFindIndicesForReconciliation(t *testing.T) {
	t.Run("When there is a conflicting index", func(t *testing.T) {
		l := &Logs{
			entries:      map[int]*LogEntry{0: {Term: 0}, 1: {Term: 1}, 2: {Term: 1}, 3: {Term: 2}, 4: {Term: 3}, 5: {Term: 3}},
			lastLogIndex: 5,
			lastLogTerm:  3,
		}

		firstConflictingIndex, _, _ :=
			l.findIndicesForReconciliation(2, []*LogEntry{{Term: 1}, {Term: 2}, {Term: 4}, {Term: 4}})

		if firstConflictingIndex != 4 {
			t.Errorf("Expected firstConflictingIndex to be %v, got %v", 4, firstConflictingIndex)
		}
	})

	t.Run("When the current logs are empty", func(t *testing.T) {
		l := &Logs{
			entries:      map[int]*LogEntry{},
			lastLogIndex: 0,
			lastLogTerm:  0,
		}

		firstConflictingIndex, lastAgreeingIndex, firstNewLogPos := l.findIndicesForReconciliation(1, []*LogEntry{{Term: 1}, {Term: 1}, {Term: 3}})

		if firstConflictingIndex != -1 {
			t.Errorf("Expected firstConflictingIndex to be %v, got %v", -1, firstConflictingIndex)
		}

		if lastAgreeingIndex != 0 {
			t.Errorf("Expected lastAgreeingIndex to be %v, got %v", 0, lastAgreeingIndex)
		}

		if firstNewLogPos != 0 {
			t.Errorf("Expected firstNewLogPos to be %v, got %v", 0, firstNewLogPos)
		}
	})

	t.Run("When there are only new logs", func(t *testing.T) {
		l := &Logs{
			entries:      map[int]*LogEntry{0: {Term: 0}, 1: {Term: 1}, 2: {Term: 1}, 3: {Term: 2}, 4: {Term: 3}, 5: {Term: 3}},
			lastLogIndex: 0,
			lastLogTerm:  0,
		}

		firstConflictingIndex, lastAgreeingIndex, firstNewLogPos := l.findIndicesForReconciliation(6, []*LogEntry{{Term: 3}})

		if firstConflictingIndex != -1 {
			t.Errorf("Expected firstConflictingIndex to be %v, got %v", -1, firstConflictingIndex)
		}

		if lastAgreeingIndex != 5 {
			t.Errorf("Expected lastAgreeingIndex to be %v, got %v", 0, lastAgreeingIndex)
		}

		if firstNewLogPos != 0 {
			t.Errorf("Expected firstNewLogPos to be %v, got %v", 0, firstNewLogPos)
		}
	})

	t.Run("When there are duplicate logs and new logs", func(t *testing.T) {
		l := &Logs{
			entries:      map[int]*LogEntry{0: {Term: 0}, 1: {Term: 1}, 2: {Term: 1}, 3: {Term: 2}, 4: {Term: 3}, 5: {Term: 3}},
			lastLogIndex: 5,
			lastLogTerm:  3,
		}

		firstConflictingIndex, lastAgreeingIndex, firstNewLogPos :=
			l.findIndicesForReconciliation(3, []*LogEntry{{Term: 2}, {Term: 3}, {Term: 3}, {Term: 4}, {Term: 5}})

		if firstConflictingIndex != -1 {
			t.Errorf("Expected firstConflictingIndex to be %v, got %v", -1, firstConflictingIndex)
		}

		if lastAgreeingIndex != 5 {
			t.Errorf("Expected lastAgreeingIndex to be %v, got %v", 0, lastAgreeingIndex)
		}

		if firstNewLogPos != 3 {
			t.Errorf("Expected firstNewLogPos to be %v, got %v", 3, firstNewLogPos)
		}
	})

	t.Run("when there are no logs to append", func(t *testing.T) {
		l := &Logs{
			entries:      map[int]*LogEntry{0: {Term: 0}, 1: {Term: 1}, 2: {Term: 1}, 3: {Term: 2}, 4: {Term: 3}, 5: {Term: 3}},
			lastLogIndex: 5,
			lastLogTerm:  3,
		}

		firstConflictingIndex, lastAgreeingIndex, firstNewLogPos :=
			l.findIndicesForReconciliation(1, []*LogEntry{{Term: 1}, {Term: 1}})

		if firstConflictingIndex != -1 {
			t.Errorf("Expected firstConflictingIndex to be %v, got %v", -1, firstConflictingIndex)
		}

		if lastAgreeingIndex != 2 {
			t.Errorf("Expected lastAgreeingIndex to be %v, got %v", 0, lastAgreeingIndex)
		}

		if firstNewLogPos != 2 {
			t.Errorf("Expected firstNewLogPos to be %v, got %v", 2, firstNewLogPos)
		}
	})

	t.Run("When the incoming logs are empty", func(t *testing.T) {
		l := &Logs{
			entries:      map[int]*LogEntry{0: {Term: 0}, 1: {Term: 1}, 2: {Term: 1}, 3: {Term: 2}, 4: {Term: 3}, 5: {Term: 3}},
			lastLogIndex: 5,
			lastLogTerm:  3,
		}

		firstConflictingIndex, lastAgreeingIndex, firstNewLogPos :=
			l.findIndicesForReconciliation(6, []*LogEntry{})

		if firstConflictingIndex != -1 {
			t.Errorf("Expected firstConflictingIndex to be %v, got %v", -1, firstConflictingIndex)
		}

		if lastAgreeingIndex != 5 {
			t.Errorf("Expected lastAgreeingIndex to be %v, got %v", 5, lastAgreeingIndex)
		}

		if firstNewLogPos != 0 {
			t.Errorf("Expected firstNewLogPos to be %v, got %v", 0, firstNewLogPos)
		}
	})
}

func TestDeleteLogsFrom(t *testing.T) {
	t.Run("Deletes logs, updates lastLogIndex and lastLogTerm", func(t *testing.T) {
		l := &Logs{
			entries:      map[int]*LogEntry{0: {Term: 0}, 1: {Term: 1}, 2: {Term: 1}, 3: {Term: 2}, 4: {Term: 3}, 5: {Term: 3}},
			lastLogIndex: 5,
			lastLogTerm:  3,
		}

		l.deleteLogsFrom(3)

		expectedLogs := map[int]*LogEntry{0: {Term: 0}, 1: {Term: 1}, 2: {Term: 1}}
		if !reflect.DeepEqual(l.entries, expectedLogs) {
			t.Errorf("Expected logs to be %v, got %v", expectedLogs, l.entries)
		}

		if l.lastLogIndex != 2 {
			t.Errorf("Expected lastLogIndex to be %v, got %v", 2, l.lastLogIndex)
		}

		if l.lastLogTerm != 1 {
			t.Errorf("Expected lastLogTerm to be %v, got %v", 1, l.lastLogTerm)
		}
	})

	t.Run("Bails when the index is too low", func(t *testing.T) {
		l := &Logs{
			entries:      map[int]*LogEntry{0: {Term: 0}, 1: {Term: 1}, 2: {Term: 1}, 3: {Term: 2}, 4: {Term: 3}, 5: {Term: 3}},
			lastLogIndex: 5,
			lastLogTerm:  3,
		}

		res := l.deleteLogsFrom(-1)

		if res != false {
			t.Errorf("Expected deleteLogsFrom to return false, got %v", res)
		}

		expectedLogs := map[int]*LogEntry{0: {Term: 0}, 1: {Term: 1}, 2: {Term: 1}, 3: {Term: 2}, 4: {Term: 3}, 5: {Term: 3}}
		if !reflect.DeepEqual(l.entries, expectedLogs) {
			t.Errorf("Expected logs to be %v, got %v", expectedLogs, l.entries)
		}

		if l.lastLogIndex != 5 {
			t.Errorf("Expected lastLogIndex to be %v, got %v", 5, l.lastLogIndex)
		}

		if l.lastLogTerm != 3 {
			t.Errorf("Expected lastLogTerm to be %v, got %v", 3, l.lastLogTerm)
		}
	})

	t.Run("Bails when the index is too high", func(t *testing.T) {
		l := &Logs{
			entries:      map[int]*LogEntry{0: {Term: 0}, 1: {Term: 1}, 2: {Term: 1}, 3: {Term: 2}, 4: {Term: 3}, 5: {Term: 3}},
			lastLogIndex: 5,
			lastLogTerm:  3,
		}

		res := l.deleteLogsFrom(100)

		if res != false {
			t.Errorf("Expected deleteLogsFrom to return false, got %v", res)
		}

		expectedLogs := map[int]*LogEntry{0: {Term: 0}, 1: {Term: 1}, 2: {Term: 1}, 3: {Term: 2}, 4: {Term: 3}, 5: {Term: 3}}
		if !reflect.DeepEqual(l.entries, expectedLogs) {
			t.Errorf("Expected logs to be %v, got %v", expectedLogs, l.entries)
		}

		if l.lastLogIndex != 5 {
			t.Errorf("Expected lastLogIndex to be %v, got %v", 5, l.lastLogIndex)
		}

		if l.lastLogTerm != 3 {
			t.Errorf("Expected lastLogTerm to be %v, got %v", 3, l.lastLogTerm)
		}
	})
}

func TestAppendNewEntries(t *testing.T) {
	t.Run("Appends new entries, updates lastLogIndex and lastLogTerm", func(t *testing.T) {
		l := &Logs{
			entries:      map[int]*LogEntry{0: {Term: 0}, 1: {Term: 1}, 2: {Term: 1}, 3: {Term: 2}, 4: {Term: 3}, 5: {Term: 3}},
			lastLogIndex: 5,
			lastLogTerm:  3,
		}

		l.appendNewEntries(6, 0, []*LogEntry{{Term: 4}, {Term: 4}, {Term: 4}})

		expectedLogs := map[int]*LogEntry{0: {Term: 0}, 1: {Term: 1}, 2: {Term: 1}, 3: {Term: 2}, 4: {Term: 3}, 5: {Term: 3}, 6: {Term: 4}, 7: {Term: 4}, 8: {Term: 4}}
		if !reflect.DeepEqual(l.entries, expectedLogs) {
			t.Errorf("Expected logs to be %v, got %v", expectedLogs, l.entries)
		}

		if l.lastLogIndex != 8 {
			t.Errorf("Expected lastLogIndex to be %v, got %v", 8, l.lastLogIndex)
		}

		if l.lastLogTerm != 4 {
			t.Errorf("Expected lastLogTerm to be %v, got %v", 4, l.lastLogTerm)
		}
	})
}

func TestTakeSnapshot(t *testing.T) {
	t.Run("When there is no snapshot yet", func(t *testing.T) {
		l := &Logs{
			entries:      map[int]*LogEntry{0: {Term: 0}, 1: {Term: 1}, 2: {Term: 1}, 3: {Term: 2}, 4: {Term: 3}, 5: {Term: 3}},
			lastLogIndex: 5,
			lastLogTerm:  3,
		}
		data := []byte("some dummy data")
		snapIndex := 4

		l.takeSnapshot(snapIndex, data)

		expectedSnapshot := &Snapshot{
			LastLogIndex: snapIndex,
			LastLogTerm:  3,
			Data:         data,
		}

		if !reflect.DeepEqual(l.snapshot, expectedSnapshot) {
			t.Errorf("Expected snapshot to be %+v, got %+v", expectedSnapshot, l.snapshot)
		}

		for i := 0; i <= snapIndex; i++ {
			if _, ok := l.entries[i]; ok {
				t.Errorf("Expected log entry %v to be deleted", i)
			}
		}

		if l.lastLogIndex != 5 {
			t.Errorf("Expected lastLogIndex to be %v, got %v", 5, l.lastLogIndex)
		}

		if l.lastLogTerm != 3 {
			t.Errorf("Expected lastLogTerm to be %v, got %v", 3, l.lastLogTerm)
		}
	})

	t.Run("When there is already another snapshot", func(t *testing.T) {
		snapshot := &Snapshot{
			LastLogIndex: 2,
			LastLogTerm:  1,
			Data:         []byte("haikyuu"),
		}
		l := &Logs{
			entries:      map[int]*LogEntry{3: {Term: 2}, 4: {Term: 3}, 5: {Term: 3}},
			lastLogIndex: 5,
			lastLogTerm:  3,
			snapshot:     snapshot,
		}
		data := []byte("some dummy data")
		snapIndex := 4

		l.takeSnapshot(snapIndex, data)

		expectedSnapshot := &Snapshot{
			LastLogIndex: snapIndex,
			LastLogTerm:  3,
			Data:         data,
		}

		if !reflect.DeepEqual(l.snapshot, expectedSnapshot) {
			t.Errorf("Expected snapshot to be %+v, got %+v", expectedSnapshot, l.snapshot)
		}

		for i := 0; i <= snapIndex; i++ {
			if _, ok := l.entries[i]; ok {
				t.Errorf("Expected log entry %v to be deleted", i)
			}
		}

		if l.lastLogIndex != 5 {
			t.Errorf("Expected lastLogIndex to be %v, got %v", 5, l.lastLogIndex)
		}

		if l.lastLogTerm != 3 {
			t.Errorf("Expected lastLogTerm to be %v, got %v", 3, l.lastLogTerm)
		}
	})

	t.Run("When all logs are deleted after taking the snapshot", func(t *testing.T) {
		l := &Logs{
			entries:      map[int]*LogEntry{3: {Term: 2}, 4: {Term: 3}, 5: {Term: 3}},
			lastLogIndex: 5,
			lastLogTerm:  3,
		}
		data := []byte("some dummy data")
		snapIndex := 5

		l.takeSnapshot(snapIndex, data)

		expectedSnapshot := &Snapshot{
			LastLogIndex: snapIndex,
			LastLogTerm:  3,
			Data:         data,
		}

		if !reflect.DeepEqual(l.snapshot, expectedSnapshot) {
			t.Errorf("Expected snapshot to be %+v, got %+v", expectedSnapshot, l.snapshot)
		}

		if len(l.entries) != 0 {
			t.Errorf("Expected logs to be empty, got %+v", l.entries)
		}

		if l.lastLogIndex != 5 {
			t.Errorf("Expected lastLogIndex to be %v, got %v", 5, l.lastLogIndex)
		}

		if l.lastLogTerm != 3 {
			t.Errorf("Expected lastLogTerm to be %v, got %v", 3, l.lastLogTerm)
		}
	})
}

func TestDeleteLogsUntil(t *testing.T) {
	t.Run("Delete logs up till the given index", func(t *testing.T) {
		l := &Logs{
			entries:      map[int]*LogEntry{0: {Term: 0}, 1: {Term: 1}, 2: {Term: 1}, 3: {Term: 2}, 4: {Term: 3}, 5: {Term: 3}},
			lastLogIndex: 5,
			lastLogTerm:  3,
		}

		l.deleteLogsUntil(3)

		expectedLogs := map[int]*LogEntry{4: {Term: 3}, 5: {Term: 3}}
		if !reflect.DeepEqual(l.entries, expectedLogs) {
			t.Errorf("Expected logs to be %v, got %v", expectedLogs, l.entries)
		}

		if l.lastLogIndex != 5 {
			t.Errorf("Expected lastLogIndex to be %v, got %v", 5, l.lastLogIndex)
		}

		if l.lastLogTerm != 3 {
			t.Errorf("Expected lastLogTerm to be %v, got %v", 3, l.lastLogTerm)
		}
	})

	t.Run("Sets lastLogIndex and lastLogTerm to the snapshot's index and term when all logs are deleted", func(t *testing.T) {
		l := &Logs{
			entries:      map[int]*LogEntry{3: {Term: 2}, 4: {Term: 3}, 5: {Term: 3}},
			lastLogIndex: 5,
			lastLogTerm:  3,
			snapshot:     &Snapshot{LastLogIndex: 2, LastLogTerm: 1},
		}

		l.deleteLogsUntil(5)

		expectedLogs := map[int]*LogEntry{}
		if !reflect.DeepEqual(l.entries, expectedLogs) {
			t.Errorf("Expected logs to be %v, got %v", expectedLogs, l.entries)
		}

		if l.lastLogIndex != 2 {
			t.Errorf("Expected lastLogIndex to be %v, got %v", 2, l.lastLogIndex)
		}

		if l.lastLogTerm != 1 {
			t.Errorf("Expected lastLogTerm to be %v, got %v", 1, l.lastLogTerm)
		}
	})

	t.Run("Sets lastLogIndex and lastLogTerm to 0 when there is no snapshot and all logs are deleted", func(t *testing.T) {
		l := &Logs{
			entries:      map[int]*LogEntry{0: {Term: 0}, 1: {Term: 1}, 2: {Term: 1}, 3: {Term: 2}, 4: {Term: 3}, 5: {Term: 3}},
			lastLogIndex: 5,
			lastLogTerm:  3,
		}

		l.deleteLogsUntil(5)

		expectedLogs := map[int]*LogEntry{}
		if !reflect.DeepEqual(l.entries, expectedLogs) {
			t.Errorf("Expected logs to be %v, got %v", expectedLogs, l.entries)
		}

		if l.lastLogIndex != 0 {
			t.Errorf("Expected lastLogIndex to be %v, got %v", 0, l.lastLogIndex)
		}

		if l.lastLogTerm != 0 {
			t.Errorf("Expected lastLogTerm to be %v, got %v", 0, l.lastLogTerm)
		}
	})
}
