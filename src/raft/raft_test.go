package raft

import "testing"

func TestSnapshot(t *testing.T) {
	t.Run("Only snapshots committed entries", func(t *testing.T) {
		cfg := make_config(t, 1, false, false)
		defer cfg.cleanup()

		server := cfg.rafts[0]
		server.logs = &Logs{
			entries:      map[int]*LogEntry{3: {Term: 2}, 4: {Term: 3}, 5: {Term: 3}, 6: {Term: 3}},
			lastLogIndex: 6,
			lastLogTerm:  3,
		}
		server.commitIndex = 4
		data := []byte("some dummy data")

		server.Snapshot(6, data)

		if server.logs.snapshot.LastLogIndex != 4 {
			t.Errorf("Expected snapshot last log index to be 4, got %d", server.logs.snapshot.LastLogIndex)
		}

		if server.logs.snapshot.LastLogTerm != 3 {
			t.Errorf("Expected snapshot last log term to be 3, got %d", server.logs.snapshot.LastLogTerm)
		}

		for i := 5; i <= 6; i++ {
			if _, ok := server.logs.entries[i]; !ok {
				t.Errorf("Expected log entry at index %d to exist", i)
			}
		}
	})
}
