package raft

import "6.824/pylog/util"

// tester pass a snapshot and its last include index of log
// raft save snapshot data and trim its log
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.getLogTermL(index)
	rf.installSnapshotL(snapshot, index, term, false)
}

func (rf *Raft) installSnapshotL(snapshot []byte,
	lastIncludedIndex, lastIncludedTerm int, apply bool) {
	if rf.lastIncludedIndex >= lastIncludedIndex {
		return
	}
	rf.printfL("before install snapshot, args index: %v, args term :%v",
		lastIncludedIndex, lastIncludedTerm)
	rf.log = rf.trimLogL(lastIncludedIndex+1, rf.getLastLogIndexL(), true)
	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm
	rf.persister.SaveStateAndSnapshot(rf.persistDataL(), snapshot)
	if apply && rf.lastApplied < lastIncludedIndex {
		rf.commitIndex = util.MaxInt(rf.commitIndex, lastIncludedIndex)
		rf.applySnapshot = true
	}
	rf.printfL("after install snapshot")
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	FullDate          []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs,
	reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm

	// is effective rpc or not
	if args.Term < rf.currentTerm {
		return
	} else if args.Term > rf.currentTerm ||
		(rf.votedFor == -1 || rf.votedFor == args.LeaderId) {
		// heartbeat
		rf.beFollowerL(args.Term, args.LeaderId, true, false)
		reply.Term = rf.currentTerm
	} else {
		rf.printfL("recive append rpc not come from leader, args: %v", args)
		return
	}

	rf.installSnapshotL(args.FullDate,
		args.LastIncludedIndex, args.LastIncludedTerm, true)
}

func (rf *Raft) sendInstallSnapshot(server int,
	args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	return rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
}

// send install snapshot rpc to follower
// and handle their reply
func (rf *Raft) handleInstallSnapshotReply(server int) {
	rf.mu.Lock()
	if rf.state != sLeader {
		rf.mu.Unlock()
		return
	}
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		FullDate:          rf.persister.ReadSnapshot(),
	}
	rf.mu.Unlock()
	reply := InstallSnapshotReply{}
	ok := rf.sendInstallSnapshot(server, &args, &reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != sLeader || rf.currentTerm != args.Term {
		return
	}
	if reply.Term > args.Term {
		rf.beFollowerL(reply.Term, -1, false, true)
		return
	}
	rf.matchIndex[server] = args.LastIncludedIndex
	rf.nextIndex[server] = args.LastIncludedIndex + 1
}

// A service wants to switch to snapshot.
// Only do so if Raft hasn't have more recent info
// since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int,
	lastIncludedIndex int, snapshot []byte) bool {
	if len(snapshot) < 1 {
		return false
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.lastApplied > lastIncludedIndex {
		return false
	}
	term := rf.getLogTermL(lastIncludedIndex)
	return term == -1 || term == lastIncludedTerm
}
