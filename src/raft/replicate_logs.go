package raft

func replicateLogsToAllServers(rf *Raft) {
	for server := range rf.peers {
		if server == rf.me {
			continue
		}

		go callAppendEntries(rf, server, false)
	}
}
