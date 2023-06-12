package raft

type LogEntry struct {
	Command interface{}
	Term    int
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

func (l *Logs) overwriteLogs(index int, entries []*LogEntry) {
	oldLastLogIndex := l.lastLogIndex
	newLastLogIndex := l.lastLogIndex
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
