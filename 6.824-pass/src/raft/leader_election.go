package raft

import (
	"math/rand"
	"sync/atomic"
	"time"
)

const (
	heartbeatInterval time.Duration = time.Millisecond *
		time.Duration(75)
)

// note. lock raft before call this func
// since last char of name is "L"
func (rf *Raft) beLeaderL() {
	rf.state = sLeader
	rf.votedFor = rf.me
	rf.election = time.Now()
	rf.persistL()
	// 2B, log
	rf.nextIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = rf.getLastLogIndexL() + 1
	}
	rf.matchIndex = make([]int, len(rf.peers))
	// 2B, heartbeat and append log entries
	rf.logReplicationOnceL()
	rf.printfL("become Leader with log: %v", rf.log)
}

func (rf *Raft) beCandidateL() {
	rf.state = sCandidate
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.election = time.Now()
	rf.persistL()
	rf.printfL("become Candidate")
}

func (rf *Raft) beFollowerL(currentTerm int, voteFor int,
	heartbeat bool, debug bool) {
	oldTerm := rf.currentTerm
	oldVote := rf.votedFor
	rf.currentTerm = currentTerm
	rf.votedFor = voteFor
	if rf.currentTerm != oldTerm || rf.votedFor != oldVote {
		rf.persistL()
	}
	if heartbeat {
		rf.election = time.Now()
	}
	rf.state = sFollower
	if debug {
		rf.printfL("become Follower")
	}
}

type RequestVoteArgs struct {
	Term        int
	CandidateId int
	// 2B
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// if this raft receive a vote request from a candidate,
// decides whether to vote for it
// note if leader not fail, would not call this func
func (rf *Raft) RequestVote(args *RequestVoteArgs,
	reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term, reply.VoteGranted = rf.currentTerm, false
	if args.Term > rf.currentTerm {
		rf.beFollowerL(args.Term, -1, false, false)
	} else if args.Term == rf.currentTerm &&
		(rf.votedFor == -1 || rf.votedFor == args.CandidateId) {
		rf.beFollowerL(args.Term, -1, false, false)
	} else {
		return
	}
	reply.Term = args.Term

	// 2B, if this rf.log up to date, vote
	if args.LastLogTerm < rf.getLastLogTermL() {
		rf.printfL("is not up to date(term),"+
			" not vote, args: %v\nlog: %v", args, rf.log)
	} else if args.LastLogTerm == rf.getLastLogTermL() &&
		args.LastLogIndex < rf.getLastLogIndexL() {
		rf.printfL("is not up to date(len),"+
			" not vote, args: %v\nlog: %v", args, rf.log)
	} else {
		rf.beFollowerL(args.Term, args.CandidateId, true, true)
		reply.VoteGranted = true
	}
}

func (rf *Raft) sendRequestVote(server int,
	args *RequestVoteArgs, reply *RequestVoteReply) bool {
	return rf.peers[server].Call("Raft.RequestVote", args, reply)
}

// send request vote rpc to other raft server
// and handle their reply
func (rf *Raft) handleRequestVoteReply(server int, nVoteGranted *int32) {
	rf.mu.Lock()
	if rf.state != sCandidate {
		rf.mu.Unlock()
		return
	}
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLogIndexL(),
		LastLogTerm:  rf.getLastLogTermL(),
	}
	rf.mu.Unlock()
	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(server, &args, &reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != sCandidate || rf.currentTerm != args.Term {
		return
	}
	rf.printfL("receives reply %v from {Node %v} args %v",
		reply, server, args)

	if reply.VoteGranted && reply.Term == rf.currentTerm {
		atomic.AddInt32(nVoteGranted, 1)
		rf.printfL("receives votes(%v)", *nVoteGranted)
		// if majority vote, be leader
		if int(*nVoteGranted) > len(rf.peers)/2 {
			rf.beLeaderL()
		}
	} else { // if someone not vote, be follower
		rf.beFollowerL(reply.Term, -1, false, true)
		rf.printfL("finds a confilct {Node %v} with reply term %v\nlog: %v",
			server, reply.Term, rf.log)
	}
}

// raft server becomes candidate, requests others to vote
// and handle others' reply.
// note this raft would recive others vote request
func (rf *Raft) newElectionL() {
	rf.beCandidateL()
	n := len(rf.peers)
	me := rf.me
	rf.printfL("new election")
	var nVoteGranted int32
	atomic.StoreInt32(&nVoteGranted, 1)
	for i := 0; i < n; i++ {
		if i == me {
			continue
		}
		go rf.handleRequestVoteReply(i, &nVoteGranted)
	}
}

func electionInterval() time.Duration {
	return time.Millisecond *
		time.Duration((rand.Intn(250) + 350))
}
