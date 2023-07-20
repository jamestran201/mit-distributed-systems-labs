package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
)

type serverState int

const (
	follower serverState = iota
	candidate
	leader
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type PersistentState struct {
	CurrentTerm  int
	LogEntries   map[int]*LogEntry
	LastLogIndex int
	LastLogTerm  int
	VotedFor     int
	Snapshot     *Snapshot
}

// A Go object implementing a single Raft peer.
type Raft struct {
	applyCh   chan ApplyMsg
	dead      int32               // set by Kill()
	me        int                 // this peer's index into peers[]
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state

	// Persistent states on all servers
	currentTerm int
	logs        *Logs
	votedFor    int

	// Available to all servers
	receivedRpcFromPeer           bool
	requestVotesResponsesReceived int
	state                         serverState
	votesReceived                 int

	// Volatile state on all servers
	commitIndex int
	lastApplied int // TODO: Need to update this around when commitIndex is updated. Confirm paper's details

	// Leader only, volatile state
	matchIndex []int
	nextIndex  []int
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Your code here (2A).
	return rf.currentTerm, rf.state == leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist(snapshot *Snapshot) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	if snapshot == nil {
		snapshot = rf.logs.snapshot
	}
	ps := PersistentState{
		CurrentTerm:  rf.currentTerm,
		LogEntries:   rf.logs.entries,
		LastLogIndex: rf.logs.lastLogIndex,
		LastLogTerm:  rf.logs.lastLogTerm,
		VotedFor:     rf.votedFor,
		Snapshot:     snapshot,
	}
	e.Encode(ps)

	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)

	debugLog(rf, fmt.Sprintf("Persisted state. Current term: %d.\nVoted for: %d.\nLogs: %v\nLast log index: %d.\nLast log term: %d\nSnapshot: %+v", rf.currentTerm, rf.votedFor, rf.logs.entries, rf.logs.lastLogIndex, rf.logs.lastLogTerm, snapshot))
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var ps PersistentState
	if err := d.Decode(&ps); err != nil {
		panic(err)
	} else {
		rf.currentTerm = ps.CurrentTerm
		rf.votedFor = ps.VotedFor
		rf.logs.entries = ps.LogEntries
		rf.logs.lastLogIndex = ps.LastLogIndex
		rf.logs.lastLogTerm = ps.LastLogTerm
		rf.logs.snapshot = ps.Snapshot

		debugLog(rf, fmt.Sprintf("Restored persistent state. Current term: %d. Voted for: %d. Last log index: %d. Last log term: %d\nSnapshot: %+v", rf.currentTerm, rf.votedFor, rf.logs.lastLogIndex, rf.logs.lastLogTerm, rf.logs.snapshot))
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	acquiredLock := rf.mu.TryLock()

	debugLog(rf, fmt.Sprintf("Taking snapshot with index %d", index))

	rf.logs.takeSnapshot(index, snapshot)
	rf.persist(rf.logs.snapshot)

	debugLog(rf, fmt.Sprintf("Took snapshot. Snapshot: %+v", snapshot))

	if acquiredLock {
		rf.mu.Unlock()
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != leader {
		return -1, -1, false
	}

	rf.logs.appendLog(command, rf.currentTerm)
	rf.persist(nil)

	debugLog(rf, fmt.Sprintf("Received command from client. Last log index: %d. Last log term: %d", rf.logs.lastLogIndex, rf.logs.lastLogTerm))

	go replicateLogsToAllServers(rf)

	return rf.logs.lastLogIndex, rf.currentTerm, true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)

	debugLogPlain(rf, "Server is killed")
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here (2A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 400 and 800 milliseconds
		ms := 600 + (rand.Int63() % 1000)
		debugLogPlain(rf, fmt.Sprintf("Current election timeout is %d milliseconds", ms))

		time.Sleep(time.Duration(ms) * time.Millisecond)
		debugLogPlain(rf, "Election timeout period elapsed")

		considerStartingElection(rf)
	}
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	handleRequestVotes(rf, args, reply)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	handleAppendEntries(rf, args, reply)
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	handleInstallSnapshot(rf, args, reply)
}

func (rf *Raft) resetToFollower(term int) {
	rf.state = follower
	rf.currentTerm = term
	rf.votedFor = -1
	rf.votesReceived = 0
	rf.requestVotesResponsesReceived = 0

	rf.persist(nil)
}

func (rf *Raft) updateCommitIndexIfPossible(logIndex int) {
	if logIndex <= rf.commitIndex {
		return
	}

	// debugLog(rf, fmt.Sprintf("Trying to access index %d. Logs: %v", logIndex, rf.logs.entries))
	if rf.logs.entryAt(logIndex).Term != rf.currentTerm {
		return
	}

	// start at 1 because we know that the current server already has a log at logIndex based on the condition above
	replicatedCount := 1
	for s := range rf.peers {
		if s == rf.me {
			continue
		}

		if rf.matchIndex[s] >= logIndex {
			replicatedCount++
		}
	}

	if replicatedCount > rf.majorityCount() {
		prevCommitIndex := rf.commitIndex
		rf.commitIndex = logIndex
		debugLog(rf, fmt.Sprintf("Commit index updated to %d. Logs %v.", rf.commitIndex, rf.logs.entries))

		rf.notifyServiceOfCommittedLog(prevCommitIndex)
	}
}

func (rf *Raft) majorityCount() int {
	return len(rf.peers) / 2
}

func (rf *Raft) notifyServiceOfCommittedLog(prevCommitIndex int) {
	for i := prevCommitIndex + 1; i <= rf.commitIndex; i++ {
		if i < 1 {
			continue
		}

		debugLog(rf, fmt.Sprintf("Notifying service of committed log at index %d", i))

		msg := ApplyMsg{
			CommandValid: true,
			Command:      rf.logs.entryAt(i).Command,
			CommandIndex: i,
		}

		debugLog(rf, fmt.Sprintf("Sending apply message to applyCh: %+v", msg))

		rf.applyCh <- msg

		debugLog(rf, fmt.Sprintf("Sent apply message to applyCh: %+v", msg))
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {

	nextIndex := make([]int, len(peers))
	for i := range nextIndex {
		nextIndex[i] = 1
	}

	rf := &Raft{
		applyCh:             applyCh,
		commitIndex:         0,
		currentTerm:         0,
		lastApplied:         0,
		logs:                makeLogs(),
		matchIndex:          make([]int, len(peers)),
		me:                  me,
		nextIndex:           nextIndex,
		peers:               peers,
		persister:           persister,
		receivedRpcFromPeer: false,
		state:               follower,
		votedFor:            -1,
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	debugLogPlain(rf, "Starting server")

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
