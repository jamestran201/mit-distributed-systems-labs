package raft

import "testing"

func TestResetToFollowerWhenReplyTermIsHigherThanCurrentTerm(t *testing.T) {
	cfg := make_config(t, 1, false, false)
	defer cfg.cleanup()

	server := cfg.rafts[0]
	server.currentTerm = 1
	server.state = candidate

	args := &RequestVoteArgs{
		Term:         1,
		CandidateId:  1,
		LastLogIndex: 0,
		LastLogTerm:  0,
	}
	reply := &RequestVoteReply{Term: 2, VoteGranted: false, RequestCompleted: true}

	client := &RequestVotesClient{server, args, reply, 1, 2}
	client.handleResponse()

	if server.state != follower {
		t.Errorf("Expected server to become follower. Got %v", server.state)
	}

	if server.currentTerm != reply.Term {
		t.Errorf("Expected term to be %d. Got %d", server.currentTerm, reply.Term)
	}
}

func TestDoesNotIncrementVotesReceivedWhenVoteNotGranted(t *testing.T) {
	cfg := make_config(t, 1, false, false)
	defer cfg.cleanup()

	server := cfg.rafts[0]
	server.currentTerm = 1
	server.state = candidate
	server.votesReceived = 1

	args := &RequestVoteArgs{
		Term:         1,
		CandidateId:  1,
		LastLogIndex: 0,
		LastLogTerm:  0,
	}
	reply := &RequestVoteReply{Term: 1, VoteGranted: false, RequestCompleted: true}

	client := &RequestVotesClient{server, args, reply, 1, 2}
	client.handleResponse()

	if server.votesReceived != 1 {
		t.Errorf("Expected number of votes received to be 1. Got %d", server.votesReceived)
	}
}

func TestIncrementsVotesReceived(t *testing.T) {
	cfg := make_config(t, 1, false, false)
	defer cfg.cleanup()

	server := cfg.rafts[0]
	server.currentTerm = 1
	server.state = candidate
	server.votesReceived = 1

	args := &RequestVoteArgs{
		Term:         1,
		CandidateId:  1,
		LastLogIndex: 0,
		LastLogTerm:  0,
	}
	reply := &RequestVoteReply{Term: 1, VoteGranted: true, RequestCompleted: true}

	client := &RequestVotesClient{server, args, reply, 1, 2}
	client.handleResponse()

	if server.votesReceived != 2 {
		t.Errorf("Expected number of votes received to be 2. Got %d", server.votesReceived)
	}
}

func TestBecomesLeaderWhenReceivedEnoughVotes(t *testing.T) {
	cfg := make_config(t, 2, false, false)
	defer cfg.cleanup()

	server := cfg.rafts[0]
	server.currentTerm = 1
	server.state = candidate
	server.votesReceived = 1
	server.logs.lastLogIndex = 3

	args := &RequestVoteArgs{
		Term:         1,
		CandidateId:  1,
		LastLogIndex: 3,
		LastLogTerm:  0,
	}
	reply := &RequestVoteReply{Term: 1, VoteGranted: true, RequestCompleted: true}

	client := &RequestVotesClient{server, args, reply, 1, 2}
	client.handleResponse()

	if server.state != leader {
		t.Errorf("Expected server to become leader. Got %v", server.state)
	}

	for i, v := range server.nextIndex {
		if v != 4 {
			t.Errorf("Expected nextIndex for server %d to be 4. Got %d", i, v)
		}
	}
}

func TestDoesNotBecomeLeaderWhenNotEnoughVotes(t *testing.T) {
	cfg := make_config(t, 5, false, false)
	defer cfg.cleanup()

	server := cfg.rafts[0]
	server.currentTerm = 1
	server.state = candidate
	server.votesReceived = 1
	server.logs.lastLogIndex = 3

	args := &RequestVoteArgs{
		Term:         1,
		CandidateId:  1,
		LastLogIndex: 3,
		LastLogTerm:  0,
	}
	reply := &RequestVoteReply{Term: 1, VoteGranted: true, RequestCompleted: true}

	client := &RequestVotesClient{server, args, reply, 1, 2}
	client.handleResponse()

	if server.votesReceived != 2 {
		t.Errorf("Expected number of votes received to be 2. Got %d", server.votesReceived)
	}

	if server.state != candidate {
		t.Errorf("Expected server remain as candidate. Got %v", server.state)
	}
}
