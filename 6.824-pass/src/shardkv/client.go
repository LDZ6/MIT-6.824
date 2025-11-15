package shardkv

import (
	"log"
	"time"

	"6.824/labrpc"
	t "6.824/pylog/template"
	"6.824/pylog/util"
	"6.824/shardctrler"
)

const (
	clientRetryInterval time.Duration = time.Millisecond *
		time.Duration(100)
)

type Clerk struct {
	t.Clerk
	sc       *shardctrler.Clerk // just query config
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd
}

func MakeClerk(ctrlers []*labrpc.ClientEnd,
	make_end func(string) *labrpc.ClientEnd) *Clerk {
	register(false)
	ck := new(Clerk)
	ck.make_end = make_end
	ck.sc = shardctrler.MakeClerk(ctrlers)
	ck.config = ck.sc.Query(-1)
	ck.Init(nil)
	return ck
}

func (ck *Clerk) RpcCall(shard int, funcName string, argsPtr t.ArgsI, emptyReplyPtr t.ReplyI) t.ReplyI {
	for { // choose one group
		gid := ck.config.Shards[shard]
		servers := targetGroupL(gid, ck.config.Groups, ck.make_end)
		if servers == nil {
			time.Sleep(clientRetryInterval)
			ck.config = ck.sc.Query(-1)
			continue
		}
		ck.AssignGroup(gid, servers)

		reply := ck.Clerk.RpcCall(funcName, argsPtr, emptyReplyPtr)
		if reply.Common().State == t.OK {
			return reply
		}
		ck.printf("info: try [gid %v shard %v]", gid, shard)
		time.Sleep(clientRetryInterval)
		if reply.Common().State == errWrongGroup {
			ck.config = ck.sc.Query(-1)
		}
	}
}

func (ck *Clerk) Get(key string) string {
	shard := key2shard(key)
	args := GetArgs{
		RpcArgs: ck.GenRpcArgs(true),
		Key:     key,
	}
	ck.printf("info: call Get [shard %v]: args %+v", shard, args)
	reply := ck.RpcCall(shard, "ShardKV.Get", &args, &GetReply{}).(*GetReply)
	ck.printf("info: return Get [shard %v]: args %+v", shard, args)
	return reply.Value
}

func (ck *Clerk) putAppend(key string, value string, isPut bool) {
	shard := key2shard(key)
	args := PutAppendArgs{
		RpcArgs: ck.GenRpcArgs(true),
		Key:     key,
		Value:   value,
		IsPut:   isPut,
	}
	ck.printf("info: call PutAppend [shard %v]: args %+v", shard, args)
	ck.RpcCall(shard, "ShardKV.PutAppend", &args, &PutAppendReply{})
	ck.printf("info: return PutAppend [shard %v]: args %+v", shard, args)
}

func (ck *Clerk) Put(key string, value string) {
	ck.putAppend(key, value, true)
}

func (ck *Clerk) Append(key string, value string) {
	ck.putAppend(key, value, false)
}

func (ck *Clerk) printf(format string, a ...interface{}) {
	if util.DebugShardKv {
		log.Printf(format, a...)
	}
}
