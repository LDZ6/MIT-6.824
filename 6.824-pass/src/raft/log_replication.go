package raft

import "6.824/pylog/util"

// as each Raft peer becomes aware that successive log entries are committed,
// the peer should send an ApplyMsg to the service (or tester) on the same server,
// via the applyCh passed to Make(). set CommandValid to true to indicate that
// the ApplyMsg contains a newly committed log entry.
// in part 2D you'll want to send other kinds of messages
// (e.g. snapshots) on the applyCh,
// but set CommandValid to false for these other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	// 2D
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Term    int
	Command interface{}
}

// note. lock raft before call this func
// since last char of name is "L"
func (rf *Raft) getLastLogIndexL() int {
	// at 2B, lastIncludedIndex always 0
	return rf.lastIncludedIndex + len(rf.log) - 1
}

func (rf *Raft) getLastLogTermL() int {
	return rf.getLogTermL(rf.getLastLogIndexL())
}

func (rf *Raft) getLogTermL(index int) int {
	realIndex := rf.realLogIndexL(index)
	if realIndex == -1 {
		return -1 // unknown
	} else if realIndex == 0 {
		return rf.lastIncludedTerm
	}
	return rf.log[realIndex].Term
}

func (rf *Raft) getLogCmdL(index int) interface{} {
	realIndex := rf.realLogIndexL(index)
	if realIndex < 1 {
		return nil // unknown
	}
	return rf.log[realIndex].Command
}

func (rf *Raft) realLogIndexL(index int) int {
	if index < rf.lastIncludedIndex || index > rf.getLastLogIndexL() {
		return -1 // unknown
	}
	return index - rf.lastIncludedIndex
}

// log[first..last], include the first and the last one
func (rf *Raft) trimLogL(firstIndex int, lastIndex int, debug bool) []LogEntry {
	trimLog := make([]LogEntry, 1)
	if firstIndex <= lastIndex {
		realFirst := util.MaxInt(1, rf.realLogIndexL(firstIndex))
		realLast := rf.realLogIndexL(lastIndex)
		if realFirst <= realLast {
			trimLog = append(trimLog, rf.log[realFirst:realLast+1]...)
		}
	}
	if debug {
		rf.printfL("TrimLog[%v:%v]: %v", firstIndex, lastIndex, trimLog)
	}
	return trimLog
}

// be careful, rpc would be received out of order
// compared to when they were sent
func (rf *Raft) appendLogArrL(firstIndex int, entries []LogEntry) (ok bool) {
	ok = false
	if len(entries) < 1 {
		return
	}
	if firstIndex <= rf.lastIncludedIndex {
		return
	}
	realFirst := firstIndex - rf.lastIncludedIndex
	oldLen := len(rf.log)
	if realFirst > oldLen {
		return
	}
	ok = true
	for i, e := range entries {
		idx := realFirst + i
		if idx < oldLen {
			rf.log[idx] = e
		} else {
			rf.log = append(rf.log, entries[i:]...)
			break
		}
	}
	next := realFirst + len(entries)
	if next > 0 && next < len(rf.log) &&
		rf.log[next].Term < rf.log[next-1].Term {
		rf.log = rf.log[:next]
	}
	rf.persistL()
	return
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
	Term               int
	FirstConflictIndex int
	ConflictTerm       int
	Success            bool
}

// if no command, heartbeat,
// else decides whether to commit it log
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term, reply.Success = rf.currentTerm, false
	reply.ConflictTerm, reply.FirstConflictIndex = 0, 0

	// is effective rpc or not
	if args.Term < rf.currentTerm {
		// DPrintf(rf.infoL()+"recive append rpc come from old term: %v", args.Term)
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

	// 2D
	if args.PrevLogIndex < rf.lastIncludedIndex {
		reply.FirstConflictIndex = rf.lastIncludedIndex + 1
		reply.ConflictTerm = -1
		rf.printfL("pre log entry had been installed. args: %v\nreply: %v", args, reply)
		return
	}

	// commit log
	if rf.getLastLogIndexL() < args.PrevLogIndex {
		lastTerm := rf.getLastLogTermL()
		conflictIndex := rf.getLastLogIndexL()
		if lastTerm == args.PrevLogTerm {
			reply.ConflictTerm = -1
		} else {
			reply.ConflictTerm = lastTerm
			for conflictIndex > rf.commitIndex &&
				rf.getLogTermL(conflictIndex) == lastTerm {
				conflictIndex -= 1
			}
		}
		reply.FirstConflictIndex = conflictIndex + 1
		rf.printfL("not include pre log entry. args: %v\nreply: %v", args, reply)
		return
	}
	prevTerm := rf.getLogTermL(args.PrevLogIndex)
	if prevTerm != args.PrevLogTerm {
		conflictIndex := args.PrevLogIndex - 1
		for conflictIndex > rf.commitIndex &&
			rf.getLogTermL(conflictIndex) == prevTerm {
			conflictIndex -= 1
		}
		reply.FirstConflictIndex = conflictIndex + 1
		reply.ConflictTerm = prevTerm
		// delete confict entry
		rf.printfL("not match pre log entry\nLog old: %v", rf.log)
		rf.log = rf.trimLogL(rf.lastIncludedIndex+1, conflictIndex, true)
		rf.persistL()
		rf.printfL("Log now: %v\nargs: %v\nreply: %v", rf.log, args, reply)
		return
	}
	// args.Pre is a match
	if rf.appendLogArrL(args.PrevLogIndex+1, args.Entries) {
		rf.printfL("append Log, args: %v\nand log now: %v", args, rf.log)
		reply.Success = true
	}

	// update commit index
	newCommit := util.MinInt(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
	if rf.commitIndex < newCommit {
		rf.commitIndex = util.MinInt(newCommit, rf.getLastLogIndexL())
		rf.printfL("commitIndex update to: %v, args: %v, reply: %v",
			rf.commitIndex, args, reply)
	}
}

func (rf *Raft) sendAppendEntries(server int,
	args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}

// send append entries rpc to follower
// and handle their reply
func (rf *Raft) handleAppendEntriesReply(server int) {
	rf.mu.Lock()
	if rf.state != sLeader {
		rf.mu.Unlock()
		return
	}
	// 2D
	if rf.nextIndex[server] <= rf.lastIncludedIndex {
		rf.mu.Unlock()
		rf.handleInstallSnapshotReply(server)
		return
	}

	if rf.nextIndex[server] > rf.getLastLogIndexL()+1 {
		rf.nextIndex[server] = rf.getLastLogIndexL() + 1
	}

	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: rf.nextIndex[server] - 1,
		PrevLogTerm:  rf.getLogTermL(rf.nextIndex[server] - 1),
		Entries:      nil,
		LeaderCommit: rf.commitIndex,
	}
	if rf.getLastLogIndexL() >= rf.nextIndex[server] {
		args.Entries = make([]LogEntry, 0)
		subLog := rf.trimLogL(rf.nextIndex[server],
			rf.getLastLogIndexL(), false)
		args.Entries = append(args.Entries, subLog[1:]...)
	}
	rf.mu.Unlock()
	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(server, &args, &reply)
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

	if reply.Success {
		rf.matchIndex[server] =
			util.MaxInt(rf.matchIndex[server], args.PrevLogIndex+len(args.Entries))
		rf.nextIndex[server] = rf.matchIndex[server] + 1
	} else if reply.FirstConflictIndex != 0 { // retry
		if reply.ConflictTerm == -1 {
			rf.nextIndex[server] = reply.FirstConflictIndex
		} else {
			i := reply.FirstConflictIndex
			for i < rf.getLastLogIndexL() {
				if rf.getLogTermL(i) != reply.ConflictTerm {
					break
				}
				i += 1
			}
			rf.nextIndex[server] = i
		}
		rf.logReplicationOnceL()
	}

	match := util.MajorityInt(rf.matchIndex)
	if match != -1 &&
		rf.getLogTermL(match) == rf.currentTerm &&
		match > rf.commitIndex {
		rf.commitIndex = match
		rf.logReplicationOnceL()
		rf.printfL("commitIndex update to: %v", rf.commitIndex)
	}
}

// leader call this func to heartbeat or commit log
func (rf *Raft) logReplicationOnceL() {
	n := len(rf.peers)
	for i := 0; i < n; i++ {
		if i == rf.me {
			continue
		}
		go rf.handleAppendEntriesReply(i)
	}
}

// if this raft peer is leader, append command to its log
// and then return index of the log where the command is located
func (rf *Raft) Start(command interface{}) (index int, term int, isLeader bool) {
	if rf.killed() {
		return -1, -1, false
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != sLeader {
		return -1, -1, false
	}
	isLeader = true
	index = rf.getLastLogIndexL() + 1
	term = rf.currentTerm
	entry := LogEntry{Term: term, Command: command}
	rf.log = append(rf.log, entry)
	rf.persistL()
	rf.printfL("start index: %v, entry: %+v", index, entry)
	rf.nextIndex[rf.me] = rf.getLastLogIndexL() + 1
	rf.matchIndex[rf.me] = rf.getLastLogIndexL()
	rf.logReplicationOnceL()
	return
}
