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
