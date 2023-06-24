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
		entries:      map[int]*LogEntry{},
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

func (l *Logs) reconcile(index int, entries []*LogEntry) {
	// debugLogForRequest(rf, traceId, fmt.Sprintf("Starting at %d", index))
	i := 0
	startIndex := index
	for ; startIndex <= l.lastLogIndex; startIndex++ {
		if i >= len(entries) {
			break
		}

		log := l.entries[startIndex]
		if log.Term != entries[i].Term {
			// debugLogForRequest(rf, traceId, "BREAKING")
			break
		}

		i++
	}

	// no new logs to append
	if i >= len(entries) {
		return
	}

	// debugLogForRequest(rf, traceId, fmt.Sprintf("startIndex: %d, i: %d", startIndex, i))
	// there could be conflicting logs, or a new log to append
	l.overwriteLogs(startIndex, entries[i:])
}

func (l *Logs) overwriteLogs(index int, entries []*LogEntry) {
	oldLastLogIndex := l.lastLogIndex
	newLastLogIndex := index - 1
	curIndex := index
	for i := 0; i < len(entries); i++ {
		l.entries[curIndex] = entries[i]
		newLastLogIndex = curIndex
		curIndex++
	}

	for i := newLastLogIndex + 1; i <= oldLastLogIndex; i++ {
		delete(l.entries, i)
	}

	l.lastLogIndex = newLastLogIndex
	l.lastLogTerm = l.entries[newLastLogIndex].Term
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
	result := -1
	for index, entry := range l.entries {
		if entry.Term == term {
			result = index
			break
		}
	}

	return result
}
