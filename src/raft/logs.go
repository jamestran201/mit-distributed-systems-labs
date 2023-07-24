package raft

func (rf *Raft) appendLogEntry(command interface{}) int {
	clonedLogs := make([]LogEntry, len(rf.logs)+1)
	for i, entry := range rf.logs {
		clonedLogs[i] = LogEntry{Command: entry.Command, Term: entry.Term}
	}

	newEntry := LogEntry{Command: command, Term: rf.currentTerm}
	clonedLogs[len(clonedLogs)-1] = newEntry

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

func (rf *Raft) replicateLogs() {
	for server := range rf.peers {
		if server == rf.me {
			continue
		}

		go rf.callAppendEntries(server)
	}
}
