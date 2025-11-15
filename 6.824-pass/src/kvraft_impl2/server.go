package kvraft_impl2

import (
	"log"

	"6.824/labrpc"
	t "6.824/pylog/template"
	"6.824/pylog/util"
	"6.824/raft"
)

type KVServer struct {
	t.Server
	rf *raft.Raft        // needed by config.go
	db map[string]string // database
}

func StartKVServer(servers []*labrpc.ClientEnd, me int,
	persister *raft.Persister, maxRaftState int) *KVServer {
	register(true)
	kv := new(KVServer)
	kv.db = map[string]string{}
	kv.Init(kv, servers, me, persister, maxRaftState)
	return kv
}

func (kv *KVServer) Raft() *raft.Raft {
	return kv.rf
}

func (kv *KVServer) SetRaft(rf *raft.Raft) {
	kv.rf = rf
}

func (kv *KVServer) printfL(format string, a ...interface{}) {
	if util.DebugKvraft {
		log.Printf(kv.InfoL()+format, a...)
	}
}
