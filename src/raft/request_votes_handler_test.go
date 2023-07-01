package raft

import "testing"

func TestRejectWhenCandidateTermIsLessThanCurrentTerm(t *testing.T) {
	cfg := make_config(t, 1, false, false)
	defer cfg.cleanup()

	server := cfg.rafts[0]
	server.currentTerm = 2
	args := &RequestVoteArgs{
		Term:         1,
		CandidateId:  2,
		LastLogIndex: 0,
		LastLogTerm:  0,
	}
	reply := &RequestVoteReply{}

	handleRequestVotes(server, args, reply)

	if reply.VoteGranted != false {
		t.Errorf("Expected vote to be not granted. Got %v", reply.VoteGranted)
	}

	if reply.Term != server.currentTerm {
		t.Errorf("Expected term to be %d. Got %d", server.currentTerm, reply.Term)
	}
}

func TestRejectWhenAlreadyVotedForAnotherCandidate(t *testing.T) {
	cfg := make_config(t, 1, false, false)
	defer cfg.cleanup()

	server := cfg.rafts[0]
	server.currentTerm = 3
	server.votedFor = 3
	args := &RequestVoteArgs{
		Term:         3,
		CandidateId:  4,
		LastLogIndex: 0,
		LastLogTerm:  0,
	}
	reply := &RequestVoteReply{}

	handleRequestVotes(server, args, reply)

	if reply.VoteGranted != false {
		t.Errorf("Expected vote to be not granted. Got %v", reply.VoteGranted)
	}

	if reply.Term != server.currentTerm {
		t.Errorf("Expected term to be %d. Got %d", server.currentTerm, reply.Term)
	}
}

func TestRejectWhenCandidateLogIsNotUpToDate(t *testing.T) {
	cfg := make_config(t, 1, false, false)
	defer cfg.cleanup()

	server := cfg.rafts[0]
	server.currentTerm = 2
	server.votedFor = 1

	// Server's last log has higher term than candidate's last log
	server.logs.lastLogIndex = 1
	server.logs.lastLogTerm = 2
	args := &RequestVoteArgs{
		Term:         3,
		CandidateId:  2,
		LastLogIndex: 1,
		LastLogTerm:  1,
	}
	reply := &RequestVoteReply{}

	handleRequestVotes(server, args, reply)

	if reply.VoteGranted != false {
		t.Errorf("Expected vote to be not granted. Got %v", reply.VoteGranted)
	}

	// Server's last log is the same as candidate's last log, but server has more logs
	server.logs.lastLogIndex = 3
	server.logs.lastLogTerm = 2
	args = &RequestVoteArgs{
		Term:         3,
		CandidateId:  2,
		LastLogIndex: 2,
		LastLogTerm:  2,
	}
	reply = &RequestVoteReply{}

	handleRequestVotes(server, args, reply)

	if reply.VoteGranted != false {
		t.Errorf("Expected vote to be not granted. Got %v", reply.VoteGranted)
	}

	// Candidate's last log has higher term than server's last log
	server.logs.lastLogIndex = 3
	server.logs.lastLogTerm = 2
	args = &RequestVoteArgs{
		Term:         3,
		CandidateId:  2,
		LastLogIndex: 1,
		LastLogTerm:  3,
	}
	reply = &RequestVoteReply{}

	handleRequestVotes(server, args, reply)

	if reply.VoteGranted != true {
		t.Errorf("Expected vote to granted. Got %v", reply.VoteGranted)
	}

	if server.votedFor != 2 {
		t.Errorf("Expected votedFor to be 2. Got %d", server.votedFor)
	}

	// Candidate's last log has the same term as server's last log, but candidate has more logs
	server.logs.lastLogIndex = 6
	server.logs.lastLogTerm = 3
	args = &RequestVoteArgs{
		Term:         3,
		CandidateId:  2,
		LastLogIndex: 9,
		LastLogTerm:  3,
	}
	reply = &RequestVoteReply{}

	handleRequestVotes(server, args, reply)

	if reply.VoteGranted != true {
		t.Errorf("Expected vote to granted. Got %v", reply.VoteGranted)
	}

	if server.votedFor != 2 {
		t.Errorf("Expected votedFor to be 2. Got %d", server.votedFor)
	}
}
