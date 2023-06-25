package raft

import "fmt"

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
	// result := -1
	result := 0
	for index, entry := range l.entries {
		if entry.Term == term {
			result = index
			break
		}
	}

	return result
}

func (l *Logs) findFirstConflictingLogIndex(startIndex int, newEntries []*LogEntry) (int, int, int) {
	i := 0
	firstConflictingIndex := -1
	lastAgreeingIndex := startIndex - 1
	firstNewLog := 0
	for ; startIndex <= l.lastLogIndex; startIndex++ {
		if i >= len(newEntries) {
			break
		}

		if l.entries[startIndex].Term != newEntries[i].Term {
			firstConflictingIndex = startIndex
			break
		}

		lastAgreeingIndex = startIndex
		firstNewLog = i + 1

		i++
	}

	return firstConflictingIndex, lastAgreeingIndex, firstNewLog
}

func (l *Logs) deleteLogsFrom(index int) {
	for i := index; i <= l.lastLogIndex; i++ {
		delete(l.entries, i)
	}

	// We are deleting all logs starting from "index", so the last log index will be the log at "index - 1"
	l.lastLogIndex = index - 1
	l.lastLogTerm = l.entries[index-1].Term
}

func (l *Logs) appendNewEntries(startIndex int, firstNewLog int, newEntries []*LogEntry) {
	for i := firstNewLog; i < len(newEntries); i++ {
		l.entries[startIndex] = newEntries[i]
		l.lastLogIndex = startIndex
		l.lastLogTerm = newEntries[i].Term

		startIndex++
	}
}
