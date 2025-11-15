package raft

import (
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/pylog/util"
)

type RaftState string

const (
	sLeader    RaftState = "Leader"
	sCandidate RaftState = "Candidate"
	sFollower  RaftState = "Follower"

	applyMsgInterval time.Duration = time.Millisecond *
		time.Duration(5)
)

// a single Raft peer
type Raft struct {
	mu            util.Mutex
	peers         []*labrpc.ClientEnd // RPC end points of all peers
	persister     *Persister          // Object to hold this peer's persisted status
	me            int                 // this peer's index into peers[]
	dead          int32               // set by Kill()
	state         RaftState
	election      time.Time
	applyChan     chan ApplyMsg
	applySnapshot bool

	// persistent state on all servers
	log               []LogEntry
	currentTerm       int
	votedFor          int
	lastIncludedIndex int
	lastIncludedTerm  int

	// volatile state on all servers
	commitIndex int
	lastApplied int

	// volatile state on leader
	nextIndex  []int
	matchIndex []int
}

// constructor
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	register()
	rf := new(Raft)
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// 2A
	rf.state = sFollower
	rf.election = time.Now()
	rf.currentTerm = 0
	rf.votedFor = -1

	// 2B, since setting by tester, start index of log is 1
	rf.log = make([]LogEntry, 1)
	rf.applyChan = applyCh
	rf.applySnapshot = false
	rf.commitIndex = 0
	rf.lastApplied = 0

	// 2D
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0

	// 2C, initialize from state persisted before a crash
	// this func would reset the struct's value
	rf.readPersistL(persister.ReadRaftState())
	rf.printfL("make raft %v", rf.me)
	rf.ticker()
	return rf
}

func (rf *Raft) ticker() {
	// 2A, election timeout
	go func() {
		for {
			t0 := time.Now()
			time.Sleep(electionInterval())
			if rf.killed() {
				break
			}
			rf.mu.Lock()
			if rf.state != sLeader && rf.election.Before(t0) {
				rf.newElectionL()
			}
			rf.mu.Unlock()
		}
	}()
	// 2B, heartbeat, append and commit log entries
	go func() {
		for !rf.killed() {
			rf.mu.Lock()
			isLeader := rf.state == sLeader
			if isLeader {
				rf.logReplicationOnceL()
			}
			rf.mu.Unlock()
			time.Sleep(heartbeatInterval)
		}
	}()
	// 2B, 2D, apply msg to state machine
	go func() {
		for !rf.killed() {
			rf.applyOneMsg()
		}
	}()
}

func (rf *Raft) applyOneMsg() {
	msg := ApplyMsg{}
	rf.mu.Lock()
	if rf.applySnapshot {
		msg.SnapshotValid = true
		msg.Snapshot = rf.persister.ReadSnapshot()
		msg.SnapshotTerm = rf.lastIncludedTerm
		msg.SnapshotIndex = rf.lastIncludedIndex
	} else { // one log entry
		if rf.lastApplied >= rf.commitIndex ||
			rf.lastApplied >= rf.getLastLogIndexL() {
			rf.mu.Unlock()
			time.Sleep(applyMsgInterval)
			return
		}
		msg.CommandValid = true
		msg.CommandIndex = rf.lastApplied + 1
		msg.Command = rf.getLogCmdL(msg.CommandIndex)
		// if cmd has been applied by installing snapshot
		if msg.Command == nil {
			rf.mu.Unlock()
			time.Sleep(applyMsgInterval)
			return
		}
	}

	// must not lock the chan
	rf.mu.Unlock()
	rf.applyChan <- msg
	rf.mu.Lock()
	if rf.applySnapshot && msg.SnapshotValid {
		rf.lastApplied = msg.SnapshotIndex
		rf.applySnapshot = false
	} else if msg.CommandValid {
		rf.lastApplied = msg.CommandIndex
	}
	rf.mu.Unlock()
}

func (rf *Raft) GetState() (currentTerm int, isleader bool) {
	rf.mu.Lock()
	currentTerm = rf.currentTerm
	isleader = rf.state == sLeader
	rf.mu.Unlock()
	return
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	rf.mu.Lock()
	rf.printfL("warning: be killed")
	rf.mu.Unlock()
}

// any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func register() {
	labgob.Register(ApplyMsg{})
	labgob.Register(LogEntry{})
	labgob.Register(AppendEntriesArgs{})
	labgob.Register(AppendEntriesReply{})
	labgob.Register(RequestVoteArgs{})
	labgob.Register(RequestVoteReply{})
	labgob.Register(InstallSnapshotArgs{})
	labgob.Register(InstallSnapshotReply{})
}

func (rf *Raft) infoL() string {
	return fmt.Sprintf("{Node %v} state/term/vote: %c/%v/%v, "+
		"commit/apply: %v/%v, "+
		"first/lastLog: %v/%v, "+
		"last include index/term: %v/%v\n",
		rf.me, rf.state[0], rf.currentTerm, rf.votedFor,
		rf.commitIndex, rf.lastApplied,
		rf.lastIncludedIndex+1, rf.getLastLogIndexL(),
		rf.lastIncludedIndex, rf.lastIncludedTerm)
}

func (rf *Raft) printfL(format string, a ...interface{}) {
	if util.DebugRaft {
		log.Printf(rf.infoL()+format, a...)
	}
}
