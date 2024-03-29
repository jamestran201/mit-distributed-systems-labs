package raft

import "fmt"

type Snapshot struct {
	LastLogIndex int
	LastLogTerm  int
	Data         []byte
}

type LogEntry struct {
	Command interface{}
	Term    int
}

func (e *LogEntry) String() string {
	return fmt.Sprintf("{Command: %v, Term: %d}", e.Command, e.Term)
}

type Logs struct {
	entries      map[int]*LogEntry
	lastLogIndex int
	lastLogTerm  int
	snapshot     *Snapshot
}

func makeLogs() *Logs {
	logs := &Logs{
		entries:      map[int]*LogEntry{0: {Term: 0}},
		lastLogIndex: 0,
		lastLogTerm:  0,
	}

	return logs
}

func (l *Logs) entryAt(index int) *LogEntry {
	return l.entries[index]
}

func (l *Logs) startingFrom(index int) []*LogEntry {
	entries := []*LogEntry{}

	for i := index; i <= l.lastLogIndex; i++ {
		entries = append(entries, l.entries[i])
	}

	return entries
}

func (l *Logs) appendLog(command interface{}, term int) {
	l.lastLogIndex++
	l.lastLogTerm = term
	l.entries[l.lastLogIndex] = &LogEntry{
		Command: command,
		Term:    term,
	}
}

func (l *Logs) firstIndexOfTerm(term int) int {
	smallestIndex := -1
	for index, entry := range l.entries {
		if entry.Term == term && (smallestIndex == -1 || index < smallestIndex) {
			smallestIndex = index
		}
	}

	return smallestIndex
}

// This function finds 3 things:
// - The first index in the current logs that conflicts with the new entries
// - The last index in the current logs that agrees with the new entries
// - The position of the first new entry that is not in the current logs
//
// This function will be called when handling AppendEntries RPCs.
// At this point, we can be sure that the current server has an entry at PrevLogIndex that matches PrevLogTerm.
// Therefore, startIndex will be PrevLogIndex + 1. This is also why lastAgreeingIndex is initialized to startIndex - 1 (i.e. PrevLogIndex).
func (l *Logs) findIndicesForReconciliation(startIndex int, newEntries []*LogEntry) (int, int, int) {
	i := 0
	firstNewLogPos := 0
	firstConflictingIndex := -1
	lastAgreeingIndex := startIndex - 1
	for ; startIndex <= l.lastLogIndex; startIndex++ {
		if i >= len(newEntries) {
			break
		}

		if l.entries[startIndex].Term != newEntries[i].Term {
			firstConflictingIndex = startIndex
			break
		}

		lastAgreeingIndex = startIndex
		firstNewLogPos = i + 1

		i++
	}

	return firstConflictingIndex, lastAgreeingIndex, firstNewLogPos
}

func (l *Logs) deleteLogsFrom(index int) bool {
	if index < 0 || index > l.lastLogIndex {
		return false
	}

	for i := index; i <= l.lastLogIndex; i++ {
		delete(l.entries, i)
	}

	// We are deleting all logs starting from "index", so the last log index will be the log at "index - 1"
	l.lastLogIndex = index - 1
	l.lastLogTerm = l.entries[index-1].Term

	return true
}

func (l *Logs) deleteLogsUntil(index int) {
	minIndex := l.minIndex()
	for i := index; i >= minIndex; i-- {
		if i > l.lastLogIndex {
			continue
		}

		delete(l.entries, i)

		if i < l.lastLogIndex {
			continue
		}

		if i == minIndex {
			if l.snapshot == nil {
				l.lastLogIndex = 0
				l.lastLogTerm = 0
			} else {
				l.lastLogIndex = l.snapshot.LastLogIndex
				l.lastLogTerm = l.snapshot.LastLogTerm
			}
		} else {
			l.lastLogIndex--
			l.lastLogTerm = l.entries[l.lastLogIndex].Term
		}
	}
}

func (l *Logs) appendNewEntries(startIndex int, firstNewLog int, newEntries []*LogEntry) bool {
	appended := false
	for i := firstNewLog; i < len(newEntries); i++ {
		l.entries[startIndex] = newEntries[i]
		l.lastLogIndex = startIndex
		l.lastLogTerm = newEntries[i].Term

		appended = true

		startIndex++
	}

	return appended
}

func (l *Logs) takeSnapshot(index int, data []byte) {
	snapshot := &Snapshot{
		LastLogIndex: index,
		LastLogTerm:  l.entries[index].Term,
		Data:         data,
	}

	l.snapshot = snapshot

	for k := range l.entries {
		if k <= index {
			delete(l.entries, k)
		}
	}
}

func (l *Logs) minIndex() int {
	res := l.lastLogIndex
	for k := range l.entries {
		if k < res {
			res = k
		}
	}

	return res
}
