package raft

import "errors"

func (rf *Raft) appendLogEntry(command interface{}) int {
	clonedLogs := rf.cloneLogs(len(rf.logs))

	newEntry := LogEntry{Command: command, Term: rf.currentTerm}
	clonedLogs = append(clonedLogs, newEntry)

	rf.logs = clonedLogs
	rf.lastLogIndex++
	rf.lastLogTerm = rf.currentTerm

	return rf.lastLogIndex
}

func (rf *Raft) hasLogWithIndexAndTerm(index, term int) (bool, string) {
	if index < 0 || index > rf.lastLogIndex {
		return false, "no_logs_at_index"
	}

	res := rf.logs[index].Term == term
	if res {
		return true, ""
	} else {
		return false, "terms_do_not_match"
	}
}

func (rf *Raft) findFirstConflictIndex(args *AppendEntriesArgs) (int, int) {
	conflictIndex := -1
	firstNewIndex := -1
	j := 0
	newEntriesLen := len(args.Entries)
	for i := args.PrevLogIndex + 1; i <= rf.lastLogIndex; i++ {
		if j == newEntriesLen {
			break
		}

		if rf.logs[i].Term != args.Entries[j].Term {
			conflictIndex = i
			firstNewIndex = j
			break
		}

		j++
	}

	if conflictIndex == -1 && j < newEntriesLen {
		firstNewIndex = j
	}

	return conflictIndex, firstNewIndex
}

func (rf *Raft) deleteAllLogsFrom(index int) error {
	if index <= rf.commitIndex {
		return errors.New("cannot delete logs that have been committed")
	}

	clonedLogs := rf.cloneLogs(index)

	rf.logs = clonedLogs
	rf.lastLogIndex = index - 1
	rf.lastLogTerm = rf.logs[index-1].Term

	return nil
}

func (rf *Raft) appendNewEntries(args *AppendEntriesArgs, firstNewIndex int) {
	clonedLogs := rf.cloneLogs(len(rf.logs))

	for i := firstNewIndex; i < len(args.Entries); i++ {
		clonedLogs = append(clonedLogs, LogEntry{Command: args.Entries[i].Command, Term: args.Entries[i].Term})
	}

	rf.logs = clonedLogs
	rf.lastLogIndex = len(rf.logs) - 1
	rf.lastLogTerm = rf.logs[rf.lastLogIndex].Term
}

func (rf *Raft) cloneLogs(newLogLength int) []LogEntry {
	clonedLogs := make([]LogEntry, newLogLength)
	for i := 0; i < newLogLength; i++ {
		clonedLogs[i] = LogEntry{Command: rf.logs[i].Command, Term: rf.logs[i].Term}
	}

	return clonedLogs
}

func (rf *Raft) replicateLogs() {
	for server := range rf.peers {
		if server == rf.me {
			continue
		}

		go rf.callAppendEntries(server, "")
	}
}
