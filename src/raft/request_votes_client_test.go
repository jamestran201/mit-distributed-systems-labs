package raft

import (
	"testing"
)

func TestAbortsWhenServerIsKilled(t *testing.T) {
	cfg := make_config(t, 1, false, false)
	defer cfg.cleanup()

	server := cfg.rafts[0]
	server.state = CANDIDATE
	server.currentTerm = 1
	server.requestVotesResponsesReceived = 0
	server.votesReceived = 1
	server.Kill()

	server.callRequestVotes(1, 2)

	expectedState := CANDIDATE
	expectedCurrentTerm := 1
	expectedVotesReceived := 1
	expectedRequestVotesResponsesReceived := 0
	if server.state != expectedState {
		t.Errorf("Expected state to be %s. Got %s", expectedState, server.state)
	}

	if server.currentTerm != expectedCurrentTerm {
		t.Errorf("Expected current term to be %d. Got %d", expectedCurrentTerm, server.currentTerm)
	}

	if server.votesReceived != expectedVotesReceived {
		t.Errorf("Expected votes received to be %d. Got %d", expectedVotesReceived, server.votesReceived)
	}

	if server.requestVotesResponsesReceived != expectedRequestVotesResponsesReceived {
		t.Errorf("Expected request votes responses received to be %d. Got %d", expectedRequestVotesResponsesReceived, server.requestVotesResponsesReceived)
	}
}

func TestAbortsWhenServerIsNotCandidate(t *testing.T) {
	cfg := make_config(t, 1, false, false)
	defer cfg.cleanup()

	server := cfg.rafts[0]
	server.state = FOLLOWER
	server.currentTerm = 1
	server.requestVotesResponsesReceived = 0
	server.votesReceived = 0
	server.Kill()

	server.callRequestVotes(1, 2)

	expectedState := FOLLOWER
	expectedCurrentTerm := 1
	expectedVotesReceived := 0
	expectedRequestVotesResponsesReceived := 0
	if server.state != expectedState {
		t.Errorf("Expected state to be %s. Got %s", expectedState, server.state)
	}

	if server.currentTerm != expectedCurrentTerm {
		t.Errorf("Expected current term to be %d. Got %d", expectedCurrentTerm, server.currentTerm)
	}

	if server.votesReceived != expectedVotesReceived {
		t.Errorf("Expected votes received to be %d. Got %d", expectedVotesReceived, server.votesReceived)
	}

	if server.requestVotesResponsesReceived != expectedRequestVotesResponsesReceived {
		t.Errorf("Expected request votes responses received to be %d. Got %d", expectedRequestVotesResponsesReceived, server.requestVotesResponsesReceived)
	}
}

func TestRaft_handleRequestVotesResponse(t *testing.T) {
	t.Run("Aborts when server state has changed from when the request was made", func(t *testing.T) {
		cfg := make_config(t, 1, false, false)
		defer cfg.cleanup()

		server := cfg.rafts[0]
		server.state = FOLLOWER
		server.votesReceived = 0

		server.handleRequestVotesResponse(1, 1, &RequestVoteArgs{}, &RequestVoteReply{})

		expectedState := FOLLOWER
		expectedVotesReceived := 0
		if server.state != expectedState {
			t.Errorf("Expected state to be %s. Got %s", expectedState, server.state)
		}

		if server.votesReceived != expectedVotesReceived {
			t.Errorf("Expected votes received to be %d. Got %d", expectedVotesReceived, server.votesReceived)
		}

		server.state = CANDIDATE
		server.currentTerm = 2
		server.votesReceived = 0

		server.handleRequestVotesResponse(1, 1, &RequestVoteArgs{}, &RequestVoteReply{})

		expectedState = CANDIDATE
		expectedCurrentTerm := 2
		expectedVotesReceived = 0
		if server.state != expectedState {
			t.Errorf("Expected state to be %s. Got %s", expectedState, server.state)
		}

		if server.currentTerm != expectedCurrentTerm {
			t.Errorf("Expected current term to be %d. Got %d", expectedCurrentTerm, server.currentTerm)
		}

		if server.votesReceived != expectedVotesReceived {
			t.Errorf("Expected votes received to be %d. Got %d", expectedVotesReceived, server.votesReceived)
		}
	})

	t.Run("Resets to follower when given term is higher", func(t *testing.T) {
		cfg := make_config(t, 1, false, false)
		defer cfg.cleanup()

		server := cfg.rafts[0]
		server.state = CANDIDATE
		server.currentTerm = 2

		reply := &RequestVoteReply{
			Term:        3,
			VoteGranted: false,
		}
		server.handleRequestVotesResponse(1, 2, &RequestVoteArgs{}, reply)

		expectedState := FOLLOWER
		expectedCurrentTerm := 3
		if server.state != expectedState {
			t.Errorf("Expected state to be %s. Got %s", expectedState, server.state)
		}

		if server.currentTerm != expectedCurrentTerm {
			t.Errorf("Expected current term to be %d. Got %d", expectedCurrentTerm, server.currentTerm)
		}
	})

	t.Run("DoesNotUpdateVotesReceivedWhenVoteIsNotGranted", func(t *testing.T) {
		cfg := make_config(t, 3, false, false)
		defer cfg.cleanup()

		server := cfg.rafts[0]
		server.state = CANDIDATE
		server.currentTerm = 2
		server.votesReceived = 1

		reply := &RequestVoteReply{
			Term:        2,
			VoteGranted: false,
		}
		server.handleRequestVotesResponse(1, 2, &RequestVoteArgs{}, reply)

		expectedState := CANDIDATE
		expectedCurrentTerm := 2
		expectedVotesReceived := 1
		if server.state != expectedState {
			t.Errorf("Expected state to be %s. Got %s", expectedState, server.state)
		}

		if server.currentTerm != expectedCurrentTerm {
			t.Errorf("Expected current term to be %d. Got %d", expectedCurrentTerm, server.currentTerm)
		}

		if server.votesReceived != expectedVotesReceived {
			t.Errorf("Expected votes received to be %d. Got %d", expectedVotesReceived, server.votesReceived)
		}
	})

	t.Run("IncrementsVotesReceivedWhenVoteIsGranted", func(t *testing.T) {
		cfg := make_config(t, 5, false, false)
		defer cfg.cleanup()

		server := cfg.rafts[0]
		server.state = CANDIDATE
		server.currentTerm = 2
		server.votesReceived = 1

		reply := &RequestVoteReply{
			Term:        2,
			VoteGranted: true,
		}
		server.handleRequestVotesResponse(1, 2, &RequestVoteArgs{}, reply)

		expectedState := CANDIDATE
		expectedCurrentTerm := 2
		expectedVotesReceived := 2
		if server.state != expectedState {
			t.Errorf("Expected state to be %s. Got %s", expectedState, server.state)
		}

		if server.currentTerm != expectedCurrentTerm {
			t.Errorf("Expected current term to be %d. Got %d", expectedCurrentTerm, server.currentTerm)
		}

		if server.votesReceived != expectedVotesReceived {
			t.Errorf("Expected votes received to be %d. Got %d", expectedVotesReceived, server.votesReceived)
		}
	})

	t.Run("ConvertsToLeaderWhenReceivedMajorityOfVotes", func(t *testing.T) {
		cfg := make_config(t, 3, false, false)
		defer cfg.cleanup()

		server := cfg.rafts[0]
		server.state = CANDIDATE
		server.currentTerm = 2
		server.lastLogIndex = 1
		server.votesReceived = 1

		reply := &RequestVoteReply{
			Term:        2,
			VoteGranted: true,
		}
		server.handleRequestVotesResponse(1, 2, &RequestVoteArgs{}, reply)

		expectedState := LEADER
		expectedCurrentTerm := 2
		if server.state != expectedState {
			t.Errorf("Expected state to be %s. Got %s", expectedState, server.state)
		}

		if server.currentTerm != expectedCurrentTerm {
			t.Errorf("Expected current term to be %d. Got %d", expectedCurrentTerm, server.currentTerm)
		}

		for index := range server.nextIndex {
			expectedNextIndex := 2
			if server.nextIndex[index] != expectedNextIndex {
				t.Errorf("Expected next index to be %d. Got %d", expectedNextIndex, server.nextIndex[index])
			}
		}

		for index := range server.matchIndex {
			expectedMatchIndex := 0
			if server.matchIndex[index] != expectedMatchIndex {
				t.Errorf("Expected match index to be %d. Got %d", expectedMatchIndex, server.matchIndex[index])
			}
		}
	})
}
