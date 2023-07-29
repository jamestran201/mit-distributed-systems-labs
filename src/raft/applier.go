package raft

import "fmt"

func (rf *Raft) applyLogs() {
	for !rf.killed() {
		debugLogPlain(rf, "Trying to acquire applyCond lock")
		rf.applyCond.L.Lock()
		debugLogPlain(rf, "Waiting for new logs to be committed")
		rf.applyCond.Wait()

		if rf.killed() {
			return
		}

		rf.mu.Lock()
		start := rf.lastApplied + 1
		end := rf.commitIndex
		rf.mu.Unlock()

		for i := start; i <= end; i++ {
			rf.mu.Lock()
			debugLog(rf, fmt.Sprintf("Notifying service of committed log at index %d", i))
			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.logs[i].Command,
				CommandIndex: i,
			}
			rf.mu.Unlock()

			rf.applyCh <- msg

			rf.mu.Lock()
			rf.lastApplied = i
			debugLog(rf, fmt.Sprintf("Service notified of committed log at index %d", i))
			rf.mu.Unlock()
		}

		rf.applyCond.L.Unlock()
		debugLog(rf, "applyCond lock released")
	}
}
