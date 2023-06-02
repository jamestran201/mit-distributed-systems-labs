package raft

func logsNoNewerThan(rf *Raft, incLogIndex, incLogTerm int) bool {
	if rf.lastLogIndex() == 0 && incLogIndex == 0 {
		return true
	}

	return incLogIndex > rf.lastLogIndex() ||
		(incLogIndex == rf.lastLogIndex() && incLogTerm >= rf.lastLogTerm())
}
