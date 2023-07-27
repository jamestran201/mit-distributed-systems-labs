package raft

import (
	"testing"
	"time"
)

func Test_applyLogs(t *testing.T) {
	t.Run("Sends committed logs, but haven't been applied to applyCh", func(t *testing.T) {
		cfg := make_config(t, 1, false, false)
		defer cfg.cleanup()

		applyCh := make(chan ApplyMsg, 2)

		server := cfg.rafts[0]
		server.currentTerm = 2
		server.logs = []LogEntry{
			{nil, 0},
			{"moo", 1},
			{"shoo", 1},
			{"loo", 2},
		}
		server.lastLogIndex = 3
		server.lastLogTerm = 2
		server.commitIndex = 3
		server.lastApplied = 1
		server.applyCh = applyCh

		go server.applyLogs()

		time.Sleep(2 * time.Second)
		server.applyCond.Signal()

		time.Sleep(2 * time.Second)
		server.Kill()

		expectedMsg := []ApplyMsg{
			{true, "shoo", 2, false, nil, 0, 0},
			{true, "loo", 3, false, nil, 0, 0},
		}
		for _, expectedMsg := range expectedMsg {
			msg := <-applyCh
			if msg.Command != expectedMsg.Command || msg.CommandIndex != expectedMsg.CommandIndex {
				t.Errorf("Expected msg to be %+v. Got %+v", expectedMsg, msg)
			}
		}

		if server.lastApplied != 3 {
			t.Errorf("Expected lastApplied to be %d. Got %d", 3, server.lastApplied)
		}
	})

	t.Run("Does nothing when lastApplied is equal to commitIndex", func(t *testing.T) {
		cfg := make_config(t, 1, false, false)
		defer cfg.cleanup()

		applyCh := make(chan ApplyMsg, 2)

		server := cfg.rafts[0]
		server.currentTerm = 2
		server.logs = []LogEntry{
			{nil, 0},
			{"moo", 1},
			{"shoo", 1},
			{"loo", 2},
		}
		server.lastLogIndex = 3
		server.lastLogTerm = 2
		server.commitIndex = 3
		server.lastApplied = 3
		server.applyCh = applyCh

		go server.applyLogs()

		time.Sleep(2 * time.Second)
		server.applyCond.Signal()

		time.Sleep(2 * time.Second)
		server.Kill()

		if len(applyCh) != 0 {
			t.Errorf("Expected applyCh to be empty. Got %+v", applyCh)
		}

		if server.lastApplied != 3 {
			t.Errorf("Expected lastApplied to be %d. Got %d", 3, server.lastApplied)
		}
	})
}
