package shardctrler

import (
	"log"

	"6.824/labrpc"
	t "6.824/pylog/template"
	"6.824/pylog/util"
	"6.824/raft"
)

type ShardCtrler struct {
	t.Server
	rf      *raft.Raft
	configs []Config // indexed by config num
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	register(true)
	sc := new(ShardCtrler)
	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}
	sc.Init(sc, servers, me, persister, -1)
	return sc
}

func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) SetRaft(rf *raft.Raft) {
	sc.rf = rf
}

func (sc *ShardCtrler) ReadPersistL(data []byte) {
}

func (sc *ShardCtrler) PersistDataL() []byte {
	return nil
}

func (sc *ShardCtrler) InfoL() string {
	return "info: [Ctrler]" + sc.Server.InfoL()
}

func (sc *ShardCtrler) printfL(format string, a ...interface{}) {
	if util.DebugShardCtrler {
		log.Printf(sc.InfoL()+format, a...)
	}
}
