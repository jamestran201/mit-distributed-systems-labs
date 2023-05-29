package raft

func logsNoNewerThan(rf *Raft, incLogIndex, incLogTerm int) bool {
	if len(rf.logs) == 0 && incLogIndex == 0 {
		return true
	}

	return incLogIndex > len(rf.logs) ||
		(incLogIndex == len(rf.logs) && incLogTerm >= rf.logs[len(rf.logs)-1].Term)
}
