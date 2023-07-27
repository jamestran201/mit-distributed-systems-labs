package raft

import "fmt"

func (rf *Raft) applyLogs() {
	for !rf.killed() {
		rf.applyCond.L.Lock()
		rf.applyCond.Wait()

		if rf.killed() {
			return
		}

		start := rf.lastApplied + 1
		end := rf.commitIndex
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
			rf.mu.Unlock()
		}
	}
}
