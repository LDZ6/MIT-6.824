package kvraft

import (
	"fmt"
	"log"
	"sync/atomic"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/pylog/util"
	"6.824/raft"
)

// command of raft's log entry
type Op struct {
	OpType      opType
	ClientId    int64
	SequenceNum int
	Key         string
	Value       string
}

type KVServer struct {
	mu      util.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32
	// 3A, database
	db map[string]string
	// [clientId] -> last sequence cmd executed successfully
	session map[int64]int
	// 3B
	maxRaftState int
	persister    *raft.Persister
}

func StartKVServer(servers []*labrpc.ClientEnd, me int,
	persister *raft.Persister, maxraftstate int) *KVServer {
	labgob.Register(Op{})
	kv := new(KVServer)
	kv.me = me
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.db = make(map[string]string)
	kv.session = make(map[int64]int)

	kv.persister = persister // 3B
	kv.maxRaftState = maxraftstate
	kv.readPersistL(persister.ReadSnapshot())

	kv.printfL("start")
	go func() {
		for !kv.killed() {
			kv.reciveRaftApply()
		}
	}()
	return kv
}

func (kv *KVServer) reciveRaftApply() {
	msg := <-kv.applyCh
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// 3A, cmd must be executed in the order
	// in which they are received
	if msg.CommandValid {
		cmd := msg.Command.(Op)
		kv.printfL("recive apply cmd: %v", cmd)
		exist := kv.searchSessionL(cmd.ClientId, cmd.SequenceNum)
		if exist {
			kv.printfL("duplicate cmd, before exec\ncmd: %+v", cmd)
			return
		}

		state := kv.execApplyCmdL(cmd)
		kv.updateSessionL(state, cmd.ClientId, cmd.SequenceNum)

		if kv.maxRaftState > 0 && // 3B
			kv.maxRaftState <= kv.persister.RaftStateSize() {
			kv.printfL("send snapshot")
			snapshot := kv.persistDataL()
			kv.rf.Snapshot(msg.CommandIndex, snapshot)
		}
	}

	// 3B
	if msg.SnapshotValid &&
		kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
		kv.readPersistL(msg.Snapshot)
	}
}

func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.mu.Lock()
	kv.rf.Kill()
	kv.printfL("be killed")
	kv.mu.Unlock()
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) printfL(format string, a ...interface{}) {
	if util.DebugKvraft {
		term, isLeader := kv.rf.GetState()
		c := 'x'
		if isLeader {
			c = 'L'
		}
		info := fmt.Sprintf("{Server %v(%c) term %v} ", kv.me, c, term)
		log.Printf(info+format, a...)
	}
}
