package template

import (
	"fmt"
	"log"
	"sync/atomic"

	"6.824/labrpc"
	"6.824/pylog/util"
	"6.824/raft"
)

type ServerI interface {
	Raft() *raft.Raft
	SetRaft(*raft.Raft)
	ExecApplyCmdL(Op) Err
	ReadPersistL([]byte)
	PersistDataL() []byte
	InfoL() string
}

type Server struct {
	si           ServerI
	mu           util.Mutex
	me           int
	dead         int32
	applyCh      chan raft.ApplyMsg
	persister    *raft.Persister
	maxRaftState int
	session      map[int64]SessionEntry
}

func (sv *Server) Init(si ServerI, servers []*labrpc.ClientEnd, me int,
	persister *raft.Persister, maxRaftState int) {
	sv.si = si
	sv.me = me
	sv.applyCh = make(chan raft.ApplyMsg)
	rf := raft.Make(servers, me, persister, sv.applyCh)
	sv.si.SetRaft(rf)
	sv.maxRaftState = maxRaftState
	sv.persister = persister
	sv.session = map[int64]SessionEntry{}
	sv.si.ReadPersistL(persister.ReadSnapshot())
	go func() {
		for !sv.Killed() {
			sv.reciveRaftApply()
		}
	}()
	sv.printfL("warning: start")
}

func (sv *Server) reciveRaftApply() {
	msg := <-sv.applyCh
	sv.Lock()
	defer sv.Unlock()

	// cmd must be executed in the order
	// in which they are received
	if msg.CommandValid {
		cmd := msg.Command.(Op)
		rpcArgs := cmd.Args.Common()

		exist := sv.searchSessionL(rpcArgs)
		if exist {
			// sv.printfL("duplicate before exec, cmd: %+v", cmd)
			return
		}

		state := sv.si.ExecApplyCmdL(cmd)
		sv.updateSessionL(cmd.Args.Common(), state)

		if sv.maxRaftState > 0 &&
			sv.maxRaftState <= sv.persister.RaftStateSize() {
			// sv.printfL("send snapshot")
			sv.si.Raft().Snapshot(msg.CommandIndex, sv.si.PersistDataL())
		}
	} else if msg.SnapshotValid &&
		sv.si.Raft().CondInstallSnapshot(
			msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot,
		) {
		sv.si.ReadPersistL(msg.Snapshot)
	}
}

func (sv *Server) RpcExec(cmd Op) Err {
	rpcArgs := cmd.Args.Common()
	sv.Lock()
	exist := sv.searchSessionL(rpcArgs)
	if exist {
		// sv.printfL("duplicate before start, cmd: %+v", cmd)
		sv.Unlock()
		return OK
	}

	_, _, isLeader := sv.si.Raft().Start(cmd)
	sv.Unlock()

	if !isLeader {
		return errWrongLeader
	}
	ok := sv.waitApply(rpcArgs)
	if ok {
		return OK
	} else {
		return errApplyTimeOut
	}
}

func (sv *Server) Lock() {
	sv.mu.Lock()
}

func (sv *Server) Unlock() {
	sv.mu.Unlock()
}

func (sv *Server) Kill() {
	atomic.StoreInt32(&sv.dead, 1)
	sv.si.Raft().Kill()
	sv.Lock()
	sv.printfL("warning: be killed")
	sv.Unlock()
}

func (sv *Server) Killed() bool {
	z := atomic.LoadInt32(&sv.dead)
	return z == 1
}

func (sv *Server) InfoL() string {
	term, isLeader := sv.si.Raft().GetState()
	c := 'x'
	if isLeader {
		c = 'L'
	}
	return fmt.Sprintf("{Server %v(%c) term %v} ", sv.me, c, term)
}

func (sv *Server) printfL(format string, a ...interface{}) {
	if util.DebugTemplate {
		log.Printf(sv.si.InfoL()+format, a...)
	}
}
