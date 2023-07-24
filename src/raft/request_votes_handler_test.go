package raft

import (
	"testing"
)

func TestRejectsRequestVoteWithLesserTerm(t *testing.T) {
	cfg := make_config(t, 1, false, false)
	defer cfg.cleanup()

	server := cfg.rafts[0]
	server.currentTerm = 2

	args := &RequestVoteArgs{
		Term:        1,
		CandidateId: 1,
	}
	reply := &RequestVoteReply{}

	server.handleRequestVotes(args, reply)

	expectedTerm := 2
	expectedVoteGranted := false
	expectedReceivedRpcFromPeer := false
	if reply.Term != expectedTerm {
		t.Errorf("Expected term to be %d. Got %d", expectedTerm, reply.Term)
	}

	if reply.VoteGranted != expectedVoteGranted {
		t.Errorf("Expected vote granted to be %v. Got %v", expectedVoteGranted, reply.VoteGranted)
	}

	if server.currentTerm != expectedTerm {
		t.Errorf("Expected current term to be %d. Got %d", expectedTerm, server.currentTerm)
	}

	if server.receivedRpcFromPeer != expectedReceivedRpcFromPeer {
		t.Errorf("Expected received rpc from peer to be %v. Got %v", expectedReceivedRpcFromPeer, server.receivedRpcFromPeer)
	}
}

func TestConvertsToFollowerWhenGivenTermIsHigher(t *testing.T) {
	cfg := make_config(t, 1, false, false)
	defer cfg.cleanup()

	server := cfg.rafts[0]
	server.currentTerm = 2
	server.state = CANDIDATE
	server.votedFor = 3
	server.receivedRpcFromPeer = true

	args := &RequestVoteArgs{
		Term:        4,
		CandidateId: 1,
	}
	reply := &RequestVoteReply{}

	server.handleRequestVotes(args, reply)

	expectedTerm := 4
	expectedState := FOLLOWER
	expectedVotedFor := 1
	if server.currentTerm != expectedTerm {
		t.Errorf("Expected current term to be %d. Got %d", expectedTerm, server.currentTerm)
	}

	if server.state != expectedState {
		t.Errorf("Expected state to be %s. Got %s", expectedState, server.state)
	}

	if server.votedFor != expectedVotedFor {
		t.Errorf("Expected voted for to be %d. Got %d", expectedVotedFor, server.votedFor)
	}
}

func TestRejectsRequestVoteWhenAlreadyVotedForAnotherCandidate(t *testing.T) {
	cfg := make_config(t, 1, false, false)
	defer cfg.cleanup()

	server := cfg.rafts[0]
	server.currentTerm = 2
	server.votedFor = 0

	args := &RequestVoteArgs{
		Term:        2,
		CandidateId: 1,
	}
	reply := &RequestVoteReply{}

	server.handleRequestVotes(args, reply)

	expectedTerm := 2
	expectedVoteGranted := false
	expectedVotedFor := 0
	if reply.Term != expectedTerm {
		t.Errorf("Expected term to be %d. Got %d", expectedTerm, reply.Term)
	}

	if reply.VoteGranted != expectedVoteGranted {
		t.Errorf("Expected vote granted to be %v. Got %v", expectedVoteGranted, reply.VoteGranted)
	}

	if server.votedFor != expectedVotedFor {
		t.Errorf("Expected voted for to be %d. Got %d", expectedVotedFor, server.votedFor)
	}
}

func TestRejectsRequestVoteWhenCandidateIsNotUpToDate(t *testing.T) {
	cfg := make_config(t, 1, false, false)
	defer cfg.cleanup()

	server := cfg.rafts[0]
	server.currentTerm = 1
	server.lastLogIndex = 5
	server.lastLogTerm = 1
	server.votedFor = -1

	args := &RequestVoteArgs{
		Term:         2,
		CandidateId:  1,
		LastLogIndex: 4,
		LastLogTerm:  1,
	}
	reply := &RequestVoteReply{}

	server.handleRequestVotes(args, reply)

	expectedTerm := 2
	expectedVoteGranted := false
	expectedVotedFor := -1
	if reply.Term != expectedTerm {
		t.Errorf("Expected term to be %d. Got %d", expectedTerm, reply.Term)
	}

	if reply.VoteGranted != expectedVoteGranted {
		t.Errorf("Expected vote granted to be %v. Got %v", expectedVoteGranted, reply.VoteGranted)
	}

	if server.votedFor != expectedVotedFor {
		t.Errorf("Expected voted for to be %d. Got %d", expectedVotedFor, server.votedFor)
	}
}

func TestGrantVoteWhenHaveNotVotedInCurrentTermAndCandidateUpToDate(t *testing.T) {
	cfg := make_config(t, 1, false, false)
	defer cfg.cleanup()

	server := cfg.rafts[0]
	server.currentTerm = 1
	server.lastLogIndex = 5
	server.lastLogTerm = 1
	server.votedFor = -1
	server.receivedRpcFromPeer = false

	args := &RequestVoteArgs{
		Term:         2,
		CandidateId:  1,
		LastLogIndex: 5,
		LastLogTerm:  1,
	}
	reply := &RequestVoteReply{}

	server.handleRequestVotes(args, reply)

	expectedTerm := 2
	expectedVoteGranted := true
	expectedVotedFor := 1
	expectedReceivedRpcFromPeer := true
	if reply.Term != expectedTerm {
		t.Errorf("Expected term to be %d. Got %d", expectedTerm, reply.Term)
	}

	if reply.VoteGranted != expectedVoteGranted {
		t.Errorf("Expected vote granted to be %v. Got %v", expectedVoteGranted, reply.VoteGranted)
	}

	if server.votedFor != expectedVotedFor {
		t.Errorf("Expected voted for to be %d. Got %d", expectedVotedFor, server.votedFor)
	}

	if server.receivedRpcFromPeer != expectedReceivedRpcFromPeer {
		t.Errorf("Expected received rpc from peer to be %v. Got %v", expectedReceivedRpcFromPeer, server.receivedRpcFromPeer)
	}
}

func TestGranVoteWhenAlreadyVotedForSameCandidateInCurrentTermAndCandidateUpToDate(t *testing.T) {
	cfg := make_config(t, 1, false, false)
	defer cfg.cleanup()

	server := cfg.rafts[0]
	server.currentTerm = 2
	server.lastLogIndex = 5
	server.lastLogTerm = 1
	server.votedFor = 1

	args := &RequestVoteArgs{
		Term:         2,
		CandidateId:  1,
		LastLogIndex: 5,
		LastLogTerm:  1,
	}
	reply := &RequestVoteReply{}

	server.handleRequestVotes(args, reply)

	expectedTerm := 2
	expectedVoteGranted := true
	expectedVotedFor := 1
	if reply.Term != expectedTerm {
		t.Errorf("Expected term to be %d. Got %d", expectedTerm, reply.Term)
	}

	if reply.VoteGranted != expectedVoteGranted {
		t.Errorf("Expected vote granted to be %v. Got %v", expectedVoteGranted, reply.VoteGranted)
	}

	if server.votedFor != expectedVotedFor {
		t.Errorf("Expected voted for to be %d. Got %d", expectedVotedFor, server.votedFor)
	}
}

func Test_isCandidateLogUpToDate(t *testing.T) {
	t.Run("Returns true when candidate has higher term", func(t *testing.T) {
		cfg := make_config(t, 1, false, false)
		defer cfg.cleanup()

		server := cfg.rafts[0]
		server.lastLogTerm = 1
		server.lastLogIndex = 5

		args := &RequestVoteArgs{
			LastLogTerm:  2,
			LastLogIndex: 5,
		}

		if !server.isCandidateLogUpToDate(args) {
			t.Errorf("Expected candidate log to be considered up-to-date")
		}
	})

	t.Run("Returns true when candidate has same term but higher index", func(t *testing.T) {
		cfg := make_config(t, 1, false, false)
		defer cfg.cleanup()

		server := cfg.rafts[0]
		server.lastLogTerm = 1
		server.lastLogIndex = 5

		args := &RequestVoteArgs{
			LastLogTerm:  1,
			LastLogIndex: 6,
		}

		if !server.isCandidateLogUpToDate(args) {
			t.Errorf("Expected candidate log to be considered up-to-date")
		}
	})

	t.Run("Returns true when candidate has same term and index", func(t *testing.T) {
		cfg := make_config(t, 1, false, false)
		defer cfg.cleanup()

		server := cfg.rafts[0]
		server.lastLogTerm = 1
		server.lastLogIndex = 5

		args := &RequestVoteArgs{
			LastLogTerm:  1,
			LastLogIndex: 5,
		}

		if !server.isCandidateLogUpToDate(args) {
			t.Errorf("Expected candidate log to be considered up-to-date")
		}
	})

	t.Run("Returns false when candidate has lower term", func(t *testing.T) {
		cfg := make_config(t, 1, false, false)
		defer cfg.cleanup()

		server := cfg.rafts[0]
		server.lastLogTerm = 2
		server.lastLogIndex = 5

		args := &RequestVoteArgs{
			LastLogTerm:  1,
			LastLogIndex: 5,
		}

		if server.isCandidateLogUpToDate(args) {
			t.Errorf("Expected candidate log to be considered not up-to-date")
		}
	})

	t.Run("Returns false when candidate has same term but lower index", func(t *testing.T) {
		cfg := make_config(t, 1, false, false)
		defer cfg.cleanup()

		server := cfg.rafts[0]
		server.lastLogTerm = 1
		server.lastLogIndex = 5

		args := &RequestVoteArgs{
			LastLogTerm:  1,
			LastLogIndex: 4,
		}

		if server.isCandidateLogUpToDate(args) {
			t.Errorf("Expected candidate log to be considered not up-to-date")
		}
	})
}
