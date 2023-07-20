package raft

import (
	"reflect"
	"testing"
)

func TestRejectsRequestWithStaleTerm(t *testing.T) {
	cfg := make_config(t, 1, false, false)
	defer cfg.cleanup()

	server := cfg.rafts[0]
	server.currentTerm = 2
	snapshot := &Snapshot{
		LastLogIndex: 10,
		LastLogTerm:  1,
		Data:         []byte("hello"),
	}
	server.logs.snapshot = snapshot

	args := &InstallSnapshotArgs{
		Term:     1,
		LeaderId: 1,
	}
	reply := &InstallSnapshotReply{}
	handler := &InstallSnapshotHandler{server, args, reply}

	handler.Run()

	if !reflect.DeepEqual(snapshot, server.logs.snapshot) {
		t.Errorf("Expected snapshot to be %v, got %v", snapshot, server.logs.snapshot)
	}
}

func TestCreatesNewSnapshotWhenLastIncludedIndexIsHigher(t *testing.T) {
	cfg := make_config(t, 1, false, false)
	defer cfg.cleanup()

	server := cfg.rafts[0]
	server.currentTerm = 1
	snapshot := &Snapshot{
		LastLogIndex: 4,
		LastLogTerm:  1,
		Data:         []byte("hello"),
	}
	server.logs.snapshot = snapshot

	args := &InstallSnapshotArgs{
		Term:              1,
		LeaderId:          1,
		LastIncludedIndex: 10,
		LastIncludedTerm:  1,
		Data:              []byte("hello"),
	}
	reply := &InstallSnapshotReply{}
	handler := &InstallSnapshotHandler{server, args, reply}

	handler.Run()

	expectedSnapshot := &Snapshot{
		LastLogIndex: 10,
		LastLogTerm:  1,
		Data:         []byte("hello"),
	}
	if !reflect.DeepEqual(expectedSnapshot, server.logs.snapshot) {
		t.Errorf("Expected snapshot to be %v, got %v", expectedSnapshot, server.logs.snapshot)
	}
}

func TestDoesNotCreateNewSnapshotIfLastIncludedIndexNotHigher(t *testing.T) {
	cfg := make_config(t, 1, false, false)
	defer cfg.cleanup()

	server := cfg.rafts[0]
	server.currentTerm = 1
	snapshot := &Snapshot{
		LastLogIndex: 11,
		LastLogTerm:  1,
		Data:         []byte("hello"),
	}
	server.logs.snapshot = snapshot

	args := &InstallSnapshotArgs{
		Term:              1,
		LeaderId:          1,
		LastIncludedIndex: 10,
		LastIncludedTerm:  1,
		Data:              []byte("hello"),
	}
	reply := &InstallSnapshotReply{}
	handler := &InstallSnapshotHandler{server, args, reply}

	handler.Run()

	expectedSnapshot := &Snapshot{
		LastLogIndex: 11,
		LastLogTerm:  1,
		Data:         []byte("hello"),
	}
	if !reflect.DeepEqual(expectedSnapshot, server.logs.snapshot) {
		t.Errorf("Expected snapshot to be %v, got %v", expectedSnapshot, server.logs.snapshot)
	}
}

func TestDiscardsAllLogsWhenCoveredByNewSnapshot(t *testing.T) {
	cfg := make_config(t, 1, false, false)
	defer cfg.cleanup()

	server := cfg.rafts[0]
	server.currentTerm = 1
	snapshot := &Snapshot{
		LastLogIndex: 4,
		LastLogTerm:  1,
		Data:         []byte("hello"),
	}
	server.logs.snapshot = snapshot
	server.logs.entries = map[int]*LogEntry{
		5: {Term: 1, Command: []byte("hello")},
		6: {Term: 1, Command: []byte("hello")},
		7: {Term: 1, Command: []byte("hello")},
		8: {Term: 1, Command: []byte("hello")},
		9: {Term: 1, Command: []byte("hello")},
	}
	server.logs.lastLogIndex = 9
	server.logs.lastLogTerm = 1

	args := &InstallSnapshotArgs{
		Term:              1,
		LeaderId:          1,
		LastIncludedIndex: 10,
		LastIncludedTerm:  1,
		Data:              []byte("hello"),
	}
	reply := &InstallSnapshotReply{}
	handler := &InstallSnapshotHandler{server, args, reply}

	handler.Run()

	expectedSnapshot := &Snapshot{
		LastLogIndex: 10,
		LastLogTerm:  1,
		Data:         []byte("hello"),
	}
	if !reflect.DeepEqual(expectedSnapshot, server.logs.snapshot) {
		t.Errorf("Expected snapshot to be %v, got %v", expectedSnapshot, server.logs.snapshot)
	}

	if len(server.logs.entries) != 0 {
		t.Errorf("Expected all logs to be discarded, remaining logs: %v", server.logs.entries)
	}

	if server.logs.lastLogIndex != 10 {
		t.Errorf("Expected lastLogIndex to be 10, got %d", server.logs.lastLogIndex)
	}

	if server.logs.lastLogTerm != 1 {
		t.Errorf("Expected lastLogTerm to be 1, got %d", server.logs.lastLogTerm)
	}
}

func TestDiscardsSomeLogsWhenPartiallyCoveredByNewSnapshot(t *testing.T) {
	cfg := make_config(t, 1, false, false)
	defer cfg.cleanup()

	server := cfg.rafts[0]
	server.currentTerm = 1
	snapshot := &Snapshot{
		LastLogIndex: 4,
		LastLogTerm:  1,
		Data:         []byte("hello"),
	}
	server.logs.snapshot = snapshot
	server.logs.entries = map[int]*LogEntry{
		5: {Term: 1, Command: []byte("hello")},
		6: {Term: 1, Command: []byte("hello")},
		7: {Term: 1, Command: []byte("hello")},
		8: {Term: 1, Command: []byte("hello")},
		9: {Term: 1, Command: []byte("hello")},
	}
	server.logs.lastLogIndex = 9
	server.logs.lastLogTerm = 1

	args := &InstallSnapshotArgs{
		Term:              1,
		LeaderId:          1,
		LastIncludedIndex: 6,
		LastIncludedTerm:  1,
		Data:              []byte("hello"),
	}
	reply := &InstallSnapshotReply{}
	handler := &InstallSnapshotHandler{server, args, reply}

	handler.Run()

	expectedSnapshot := &Snapshot{
		LastLogIndex: 6,
		LastLogTerm:  1,
		Data:         []byte("hello"),
	}
	if !reflect.DeepEqual(expectedSnapshot, server.logs.snapshot) {
		t.Errorf("Expected snapshot to be %v, got %v", expectedSnapshot, server.logs.snapshot)
	}

	expectedLogs := map[int]*LogEntry{
		7: {Term: 1, Command: []byte("hello")},
		8: {Term: 1, Command: []byte("hello")},
		9: {Term: 1, Command: []byte("hello")},
	}
	if !reflect.DeepEqual(expectedLogs, server.logs.entries) {
		t.Errorf("Expected logs to be %v, got %v", expectedLogs, server.logs.entries)
	}

	if server.logs.lastLogIndex != 9 {
		t.Errorf("Expected lastLogIndex to be 9, got %d", server.logs.lastLogIndex)
	}

	if server.logs.lastLogTerm != 1 {
		t.Errorf("Expected lastLogTerm to be 1, got %d", server.logs.lastLogTerm)
	}
}
