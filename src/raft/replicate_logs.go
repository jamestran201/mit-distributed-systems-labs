package raft

// import "fmt"

// func replicateLogsToAllServers(rf *Raft) {
// 	for i := range rf.peers {
// 		if i == rf.me {
// 			continue
// 		}

// 		go replicateLogsToServer(rf, i)
// 	}
// }

// func replicateLogsToServer(rf *Raft, server int) {
// 	if rf.killed() {
// 		return
// 	}

// 	shouldExit := false
// 	var args *AppendEntriesArgs
// 	withLock(&rf.mu, func() {
// 		if rf.state != leader {
// 			shouldExit = true
// 			return
// 		}

// 		args = &AppendEntriesArgs{
// 			Term:         rf.currentTerm,
// 			LeaderId:     rf.me,
// 			PrevLogIndex: prevLogIndexForServer(rf, server),
// 			PrevLogTerm:  prevLogTermForServer(rf, server),
// 			Entries:      logEntriesToSend(rf, server),
// 			LeaderCommit: rf.commitIndex,
// 		}
// 	})

// 	if shouldExit {
// 		return
// 	}

// 	reply := &AppendEntriesReply{}
// 	sendAppendEntries(rf, server, args, reply)
// 	handleReplicateLogsResponse(rf, server, reply, args.PrevLogIndex, len(args.Entries))
// }

func prevLogIndexForServer(rf *Raft, server int) int {
	return rf.nextIndex[server] - 1
}

func prevLogTermForServer(rf *Raft, server int) int {
	logEntry := rf.logs.entryAt(prevLogIndexForServer(rf, server))
	if logEntry == nil {
		return 0
	} else {
		return logEntry.Term
	}
}

func logEntriesToSend(rf *Raft, server int) []*LogEntry {
	return rf.logs.startingFrom(rf.nextIndex[server])
}

// func handleReplicateLogsResponse(rf *Raft, server int, reply *AppendEntriesReply, prevLogIndex int, numEntries int) {
// 	if rf.killed() {
// 		return
// 	}

// 	rf.mu.Lock()
// 	defer rf.mu.Unlock()

// 	if rf.state != leader {
// 		debugLog(rf, "Server is no longer the leader, skip handling response for logs replication")
// 		return
// 	}

// 	if !reply.RequestCompleted {
// 		debugLog(rf, "A request for logs replication could not be processed successfully")
// 		go replicateLogsToServer(rf, server)
// 		return
// 	}

// 	if reply.Term > rf.currentTerm {
// 		debugLog(rf, "Received AppendEntries response from server with higher term. Reset state to follower")
// 		rf.resetToFollower(reply.Term)
// 		return
// 	}

// 	if reply.Success {
// 		rf.nextIndex[server] = prevLogIndex + numEntries + 1
// 		rf.matchIndex[server] = prevLogIndex + numEntries
// 		debugLog(rf, fmt.Sprintf("Replicated logs to server %d successfully", server))
// 	} else {
// 		rf.nextIndex[server]--
// 		debugLog(rf, fmt.Sprintf("Failed to replicate logs to server %d. Retrying...", server))
// 		go replicateLogsToServer(rf, server)
// 	}
// }
