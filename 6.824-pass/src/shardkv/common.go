package shardkv

import (
	"6.824/labgob"
	"6.824/labrpc"
	t "6.824/pylog/template"
	sc "6.824/shardctrler"
)

func targetGroupL(gid int, groups map[int][]string,
	make_end func(string) *labrpc.ClientEnd) []*labrpc.ClientEnd {
	serverStringArr, exist := groups[gid]
	if !exist {
		return nil
	}
	serverEndArr := []*labrpc.ClientEnd{}
	for _, s := range serverStringArr {
		serverEndArr = append(serverEndArr, make_end(s))
	}
	return serverEndArr
}

func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= sc.NShards
	return shard
}

const (
	errWrongGroup    t.Err = "ErrWrongGroup"
	errShardNotReady t.Err = "ErrShardNotReady"
)

const (
	cmdGet    t.OpType = "Get"
	cmdPut    t.OpType = "Put"
	cmdAppend t.OpType = "Append"
)

type GetArgs struct {
	t.RpcArgs
	Key string
}

type GetReply struct {
	t.RpcReply
	Value string
}

type PutAppendArgs struct {
	t.RpcArgs
	Key   string
	Value string
	IsPut bool
}

type PutAppendReply struct {
	t.RpcReply
}

func register(isServer bool) {
	t.Register(isServer)
	if isServer {
		labgob.Register(sc.Config{})
		labgob.Register(ShardEntry{})
		labgob.Register(MigrateShardArgs{})
		labgob.Register(MigrateShardReply{})
		labgob.Register(SyncCfgArgs{})
		labgob.Register(GcShardArgs{})
	}
	labgob.Register(GetArgs{})
	labgob.Register(GetReply{})
	labgob.Register(PutAppendArgs{})
	labgob.Register(PutAppendReply{})
}
