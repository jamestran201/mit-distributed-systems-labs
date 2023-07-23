package raft

import "testing"

func TestRejection(t *testing.T) {
	t.Run("Rejects when given term is less than current term", func(t *testing.T) {
		cfg := make_config(t, 1, false, false)
		defer cfg.cleanup()

		server := cfg.rafts[0]
		server.currentTerm = 2

		args := &AppendEntriesArgs{
			Term:     1,
			LeaderId: 1,
		}
		reply := &AppendEntriesReply{}

		server.handleAppendEntries(args, reply)

		expectedTerm := 2
		expectedSuccess := false
		if reply.Term != expectedTerm {
			t.Errorf("Expected term to be %d. Got %d", expectedTerm, reply.Term)
		}

		if reply.Success != expectedSuccess {
			t.Errorf("Expected success to be %v. Got %v", expectedSuccess, reply.Success)
		}
	})
}

func TestResetsToFollowerWhenHandlingAppendEntries(t *testing.T) {
	cfg := make_config(t, 1, false, false)
	defer cfg.cleanup()

	server := cfg.rafts[0]
	server.currentTerm = 2
	server.state = LEADER

	// Test when given term is greater than current term
	args := &AppendEntriesArgs{
		Term:     4,
		LeaderId: 1,
	}
	reply := &AppendEntriesReply{}

	server.handleAppendEntries(args, reply)

	expectedTerm := 4
	expectedState := FOLLOWER
	if server.currentTerm != expectedTerm {
		t.Errorf("Expected current term to be %d. Got %d", expectedTerm, server.currentTerm)
	}

	if server.state != expectedState {
		t.Errorf("Expected state to be %s. Got %s", expectedState, server.state)
	}

	// Test when candidate receives AppendEntries from leader
	server.currentTerm = 4
	server.state = CANDIDATE

	server.handleAppendEntries(args, reply)

	expectedTerm = 4
	expectedState = FOLLOWER
	if server.currentTerm != expectedTerm {
		t.Errorf("Expected current term to be %d. Got %d", expectedTerm, server.currentTerm)
	}

	if server.state != expectedState {
		t.Errorf("Expected state to be %s. Got %s", expectedState, server.state)
	}
}

func TestSetsReceivedRpcFromPeerToTrueWhenAppendEntriesIsValid(t *testing.T) {
	cfg := make_config(t, 1, false, false)
	defer cfg.cleanup()

	server := cfg.rafts[0]
	server.currentTerm = 2
	server.state = FOLLOWER
	server.receivedRpcFromPeer = false

	args := &AppendEntriesArgs{
		Term:     2,
		LeaderId: 1,
	}
	reply := &AppendEntriesReply{}

	server.handleAppendEntries(args, reply)

	expectedReceivedRpcFromPeer := true
	if server.receivedRpcFromPeer != expectedReceivedRpcFromPeer {
		t.Errorf("Expected received rpc from peer to be %v. Got %v", expectedReceivedRpcFromPeer, server.receivedRpcFromPeer)
	}
}
