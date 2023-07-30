package raft

import "fmt"

func (rf *Raft) applyLogs() {
	if rf.killed() {
		return
	}

	rf.mu.Lock()
	if rf.lastApplied >= rf.commitIndex {
		debugLog(rf, "Aborting newApplyLogs because all logs are applied")
		rf.mu.Unlock()
		return
	}

	start := rf.lastApplied + 1
	end := rf.commitIndex
	rf.mu.Unlock()

	for i := start; i <= end; i++ {
		// Checks before notifying service
		rf.mu.Lock()
		if rf.killed() {
			rf.mu.Unlock()
			return
		}
		if rf.lastApplied >= i {
			rf.mu.Unlock()
			continue
		}

		debugLog(rf, fmt.Sprintf("Notifying service of committed log at index %d", i))
		msg := ApplyMsg{
			CommandValid: true,
			Command:      rf.logs[i].Command,
			CommandIndex: i,
		}
		rf.mu.Unlock()

		// Notify service
		rf.applyMu.Lock()
		rf.applyCh <- msg
		rf.applyMu.Unlock()

		// Checks before updating lastApplied
		rf.mu.Lock()
		if rf.killed() {
			rf.mu.Unlock()
			return
		}
		if rf.lastApplied >= i {
			rf.mu.Unlock()
			continue
		}

		rf.lastApplied = i
		debugLog(rf, fmt.Sprintf("Service notified of committed log at index %d", i))
		rf.mu.Unlock()
	}
}
