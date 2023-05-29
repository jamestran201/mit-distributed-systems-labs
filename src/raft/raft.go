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
	//	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
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

type LogEntry struct {
	Command interface{}
	Term    int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm         int
	votedFor            int
	state               serverState
	receivedRpcFromPeer bool
	logs                []LogEntry

	votesReceived                 int
	requestVotesResponsesReceived int

	// Available to all servers
	commitIndex int
	lastApplied int

	// Leader only
	nextIndex  []int
	matchIndex []int
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
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
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

	debugLog(rf, "Server is killed")
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
		ms := 400 + (rand.Int63() % 800)
		debugLog(rf, fmt.Sprintf("Current election timeout is %d milliseconds", ms))

		time.Sleep(time.Duration(ms) * time.Millisecond)
		debugLog(rf, "Election timeout period elapsed")

		considerStartingElection(rf)
	}
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	handleRequestVotes(rf, args, reply)
}

func (rf *Raft) appendEntriesFanout(term int) {
	var prevStopCh chan bool
	for !rf.killed() {
		debugLog(rf, "Sending AppendEntries to followers")

		if prevStopCh != nil {
			prevStopCh <- true
		}

		responseCh := make(chan *AppendEntriesReply, len(rf.peers)-1)
		for i := range rf.peers {
			if rf.killed() {
				return
			}

			if i == rf.me {
				continue
			}

			shouldExit := false
			withLock(&rf.mu, func() {
				shouldExit = rf.state != leader
				if shouldExit {
					debugLog(rf, "Exiting AppendEntriesFanout routine because server is no longer the leader")
					return
				}

				prevLogTerm := 0
				if len(rf.logs) > 0 {
					prevLogTerm = rf.logs[len(rf.logs)-1].Term
				}

				args := &AppendEntriesArgs{
					Term:         term,
					LeaderId:     rf.me,
					PrevLogIndex: len(rf.logs),
					PrevLogTerm:  prevLogTerm,
					Entries:      []LogEntry{},
				}
				go rf.sendAppendEntries(i, responseCh, args, &AppendEntriesReply{})
			})

			if shouldExit {
				return
			}
		}

		prevStopCh = make(chan bool, 1)
		go rf.handleAppendEntriesResponses(term, responseCh, prevStopCh)

		time.Sleep(300 * time.Millisecond)
	}
}

func (rf *Raft) handleAppendEntriesResponses(term int, responseCh chan *AppendEntriesReply, stopCh chan bool) {
	responsesReceived := 0
	for !rf.killed() && responsesReceived < len(rf.peers)-1 {
		select {
		case shouldExit := <-stopCh:
			if shouldExit {
				debugLog(rf, "Stopping old handleAppendEntriesResponses routine")
				return
			}
		default:
		}

		shouldExit := false
		shouldSkip := false

		// TODO: handle log inconsistencies with followers

		withLock(&rf.mu, func() {
			if rf.state != leader {
				debugLog(rf, "Server is no longer the leader, exiting handleAppendEntriesResponses routine")
				shouldExit = true
				return
			}

			select {
			case reply := <-responseCh:
				responsesReceived++

				if !reply.RequestCompleted {
					debugLog(rf, "An AppendEntry request could not be processed successfully, skipping")
					shouldSkip = true
					return
				}

				if reply.Term > rf.currentTerm {
					debugLog(rf, "Received AppendEntries response from server with higher term. Reset state to follower")
					rf.resetToFollower(reply.Term)
					shouldExit = true
					return
				}
			default:
			}
		})

		if shouldExit {
			return
		}

		if shouldSkip {
			continue
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, ch chan *AppendEntriesReply, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.killed() {
		return
	}

	debugLog(rf, fmt.Sprintf("Sending AppendEntries to %d", server))

	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	reply.RequestCompleted = ok

	ch <- reply
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term             int
	Success          bool
	RequestCompleted bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	debugLog(rf, fmt.Sprintf("Received AppendEntries from %d", args.LeaderId))

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	rf.receivedRpcFromPeer = true

	if args.Term > rf.currentTerm || rf.state == candidate {
		debugLog(rf, "Received AppendEntries from server with higher term. Reset state to follower")
		rf.resetToFollower(args.Term)
	}

	if args.PrevLogIndex == 0 && len(rf.logs) > 0 {
		debugLog(rf, fmt.Sprintf("BIG WARNING!!! This server contains logs while leader %d has none. This should not happen!", args.LeaderId))
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if args.PrevLogIndex > len(rf.logs) {
		debugLog(rf, fmt.Sprintf("The logs from current server are not in sync with leader %d", args.LeaderId))
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// At this point, there are 2 possibilities:
	// 1. PrevLogIndex is 0 and the server has no logs
	// 2. PrevLogIndex is > 0 and the server has at least PrevLogIndex logs
	// This condition follows case 2
	// Use PrevLogIndex - 1 because log indices start from 1
	if args.PrevLogIndex > 0 && args.PrevLogTerm != rf.logs[args.PrevLogIndex-1].Term {
		debugLog(rf, fmt.Sprintf("The logs from current server does not have the same term as leader %d", args.LeaderId))
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// The log at rf.logs[PrevLogIndex-1] is the last one on this server that matches the leader's log.
	// All logs after this differ from the leader's and should be discarded.
	if len(rf.logs) > args.PrevLogIndex {
		rf.logs = rf.logs[:args.PrevLogIndex]
	}

	rf.logs = append(rf.logs, args.Entries...)

	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit > len(rf.logs) {
			rf.commitIndex = len(rf.logs)
		} else {
			rf.commitIndex = args.LeaderCommit
		}
	}

	reply.Term = rf.currentTerm
	reply.Success = true
}

func (rf *Raft) resetToFollower(term int) {
	rf.state = follower
	rf.currentTerm = term
	rf.votedFor = -1
	rf.votesReceived = 0
	rf.requestVotesResponsesReceived = 0
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = follower
	rf.receivedRpcFromPeer = false
	rf.logs = []LogEntry{}

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = 1
	}

	rf.matchIndex = make([]int, len(peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	debugLog(rf, "Starting server")

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
