package shardkv

import (
	"fmt"
	"log"
	"time"

	"6.824/labrpc"
	t "6.824/pylog/template"
	"6.824/pylog/util"
	"6.824/raft"
	sc "6.824/shardctrler"
)

type ShardEntry struct {
	Db     map[string]string
	CfgNum int
}

func (entry ShardEntry) clone() ShardEntry {
	clone := ShardEntry{
		Db:     make(map[string]string, len(entry.Db)),
		CfgNum: entry.CfgNum,
	}
	for k, v := range entry.Db {
		clone.Db[k] = v
	}
	return clone
}

type ShardKV struct {
	t.Server
	t.Clerk            // chat with other group
	sc       *sc.Clerk // just query config
	rf       *raft.Raft
	gid      int
	make_end func(string) *labrpc.ClientEnd
	config   sc.Config
	shardArr [sc.NShards]ShardEntry // shard(id) -> entry
}

func StartServer(servers []*labrpc.ClientEnd, me int,
	persister *raft.Persister, maxRaftState int, gid int,
	ctrlers []*labrpc.ClientEnd,
	make_end func(string) *labrpc.ClientEnd) *ShardKV {
	register(true)
	kv := new(ShardKV)
	kv.make_end = make_end
	kv.gid = gid
	kv.sc = sc.MakeClerk(ctrlers)
	kv.shardArr = [sc.NShards]ShardEntry{}
	kv.Server.Init(kv, servers, me, persister, maxRaftState)
	kv.Clerk.Init(nil)
	kv.ticker()
	return kv
}

func (kv *ShardKV) ticker() {
	go func() {
		for !kv.Killed() {
			kv.updateConfigOnce()
			time.Sleep(clientRetryInterval)
		}
	}()

	go func() {
		for !kv.Killed() {
			ok := kv.pushShardOnce()
			if !ok {
				time.Sleep(pushShardInterval)
			}
		}
	}()
}

func (kv *ShardKV) belongToThisL(shard int) bool {
	return kv.gid == kv.config.Shards[shard]
}

func (kv *ShardKV) Raft() *raft.Raft {
	return kv.rf
}

func (kv *ShardKV) SetRaft(rf *raft.Raft) {
	kv.rf = rf
}

func (kv *ShardKV) InfoL() string {
	return fmt.Sprintf("[Group %v cfg %v]", kv.gid, kv.config.Num) +
		kv.Server.InfoL()
}

func (kv *ShardKV) printfL(format string, a ...interface{}) (n int, err error) {
	if util.DebugShardKv {
		_, isLeader := kv.Raft().GetState()
		if isLeader {
			log.Printf(kv.InfoL()+format, a...)
		}
	}
	return
}
