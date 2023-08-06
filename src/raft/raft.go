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
	"sync"
	"sync/atomic"

	"6.5840/labgob"
	"6.5840/labrpc"
)

const (
	CANDIDATE = "candidate"
	FOLLOWER  = "follower"
	LEADER    = "leader"
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

type LogEntry struct {
	Command interface{}
	Term    int
}

type Snapshot struct {
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

// A Go object implementing a single Raft peer.
type Raft struct {
	applyCh   chan ApplyMsg
	applyMu   sync.Mutex
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Persistent states
	currentTerm int
	votedFor    int
	logs        []LogEntry
	snapshot    *Snapshot

	// Volatile states
	commitIndex                   int
	lastApplied                   int
	lastLogIndex                  int
	lastLogTerm                   int
	receivedRpcFromPeer           bool
	state                         string
	votesReceived                 int
	requestVotesResponsesReceived int

	// Volatile states on leaders
	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm, rf.state == LEADER
}

// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	l := rf.cloneLogs(len(rf.logs))
	ps := PersistedState{
		CurrentTerm: rf.currentTerm,
		Logs:        l,
		VotedFor:    rf.votedFor,
	}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	err := e.Encode(ps)
	if err != nil {
		panic(err)
	}

	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)

	debugLog(rf, fmt.Sprintf("Persisted state: %+v", ps))
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if data == nil || len(data) < 1 { // bootstrap without any state?
		debugLog(rf, "No persisted state found")
		return
	}

	var ps PersistedState
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	err := d.Decode(&ps)
	if err != nil {
		panic(err)
	}

	debugLog(rf, fmt.Sprintf("Read persisted state: %+v", ps))

	rf.currentTerm = ps.CurrentTerm
	rf.votedFor = ps.VotedFor

	logLength := len(ps.Logs)
	clonedLogs := make([]LogEntry, logLength)
	for i := 0; i < logLength; i++ {
		clonedLogs[i] = LogEntry{Command: ps.Logs[i].Command, Term: ps.Logs[i].Term}
	}
	rf.logs = clonedLogs
	rf.lastLogIndex = logLength - 1
	rf.lastLogTerm = rf.logs[rf.lastLogIndex].Term

	debugLog(rf, fmt.Sprintf("Restored server state. Logs: %+v", rf.logs))
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.handleRequestVotes(args, reply)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.handleAppendEntries(args, reply)
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

	if rf.state != LEADER {
		return -1, -1, false
	}

	index := rf.appendLogEntry(command)
	debugLog(rf, fmt.Sprintf("Received command from client. Last log index: %d. Last log term: %d", rf.lastLogIndex, rf.lastLogTerm))

	rf.persist()
	rf.replicateLogs()

	return index, rf.currentTerm, true
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

func (rf *Raft) resetToFollower(term int) {
	rf.currentTerm = term
	rf.requestVotesResponsesReceived = 0
	rf.state = FOLLOWER
	rf.votedFor = -1
	rf.votesReceived = 0

	rf.persist()
}

func (rf *Raft) majorityCount() int {
	return len(rf.peers)/2 + 1
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
	rf := &Raft{
		applyCh:                       applyCh,
		commitIndex:                   0,
		currentTerm:                   0,
		me:                            me,
		lastApplied:                   0,
		lastLogIndex:                  0,
		lastLogTerm:                   0,
		logs:                          []LogEntry{{Command: nil, Term: 0}},
		peers:                         peers,
		persister:                     persister,
		receivedRpcFromPeer:           false,
		requestVotesResponsesReceived: 0,
		snapshot:                      nil,
		state:                         FOLLOWER,
		votedFor:                      -1,
		votesReceived:                 0,
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.electionTimer()

	return rf
}
