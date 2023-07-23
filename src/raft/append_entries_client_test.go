package raft

import "testing"

func TestRaft_handleAppendEntriesResponse(t *testing.T) {
	// t.Run("Aborts when server state has changed from when the request was made", func(t *testing.T) {
	// 	cfg := make_config(t, 1, false, false)
	// 	defer cfg.cleanup()

	// 	server := cfg.rafts[0]
	// 	server.state = FOLLOWER
	// 	server.votesReceived = 0

	// 	server.handleAppendEntriesResponse(1, &AppendEntriesArgs{}, &AppendEntriesReply{})

	// 	// TODO: Need assertions here
	// })

	t.Run("Resets to follower when given term is higher", func(t *testing.T) {
		cfg := make_config(t, 1, false, false)
		defer cfg.cleanup()

		server := cfg.rafts[0]
		server.state = LEADER
		server.currentTerm = 2

		reply := &AppendEntriesReply{
			Term:    4,
			Success: false,
		}
		server.handleAppendEntriesResponse(1, &AppendEntriesArgs{}, reply)

		expectedState := FOLLOWER
		expectedCurrentTerm := 4
		if server.state != expectedState {
			t.Errorf("Expected state to be %s. Got %s", expectedState, server.state)
		}

		if server.currentTerm != expectedCurrentTerm {
			t.Errorf("Expected current term to be %d. Got %d", expectedCurrentTerm, server.currentTerm)
		}
	})
}
