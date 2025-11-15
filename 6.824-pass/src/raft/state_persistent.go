package raft

import (
	"bytes"

	"6.824/labgob"
	"6.824/pylog/util"
)

// for 2D, extract this function
func (rf *Raft) persistDataL() []byte {
	writebuf := new(bytes.Buffer)
	labenc := labgob.NewEncoder(writebuf)
	labenc.Encode(rf.currentTerm)
	labenc.Encode(rf.votedFor)
	labenc.Encode(rf.lastIncludedIndex)
	labenc.Encode(rf.lastIncludedTerm)
	labenc.Encode(rf.log)
	return writebuf.Bytes()
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
func (rf *Raft) persistL() {
	date := rf.persistDataL()
	rf.persister.SaveRaftState(date)
}

// restore previously persisted state.
func (rf *Raft) readPersistL(data []byte) {
	if len(data) < 1 {
		return
	}
	readbuf := bytes.NewBuffer(data)
	labdec := labgob.NewDecoder(readbuf)

	var currentTerm int
	labdec.Decode(&currentTerm)

	var votedFor int
	labdec.Decode(&votedFor)

	var lastIncludedIndex int
	labdec.Decode(&lastIncludedIndex)

	var lastIncludedTerm int
	labdec.Decode(&lastIncludedTerm)

	var log []LogEntry
	labdec.Decode(&log)

	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm
	rf.lastApplied = lastIncludedIndex
	rf.commitIndex = util.MaxInt(rf.commitIndex, lastIncludedIndex)
	rf.log = log
}
