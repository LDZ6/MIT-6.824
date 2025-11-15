package shardctrler

import (
	"6.824/labrpc"
	t "6.824/pylog/template"
)

type Clerk struct {
	t.Clerk
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	register(false)
	ck := new(Clerk)
	ck.Init(servers)
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := QueryArgs{
		RpcArgs: ck.GenRpcArgs(true),
		Num:     num,
	}
	reply := ck.RpcCall("ShardCtrler.Query", &args, &QueryReply{}).(*QueryReply)
	return reply.Config
}

func (ck *Clerk) Move(shard int, gid int) {
	args := MoveArgs{
		RpcArgs: ck.GenRpcArgs(true),
		Shard:   shard,
		GID:     gid,
	}
	ck.RpcCall("ShardCtrler.Move", &args, &MoveReply{})
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := JoinArgs{
		RpcArgs: ck.GenRpcArgs(true),
		Servers: servers,
	}
	ck.RpcCall("ShardCtrler.Join", &args, &JoinReply{})
}

func (ck *Clerk) Leave(gids []int) {
	args := LeaveArgs{
		RpcArgs: ck.GenRpcArgs(true),
		GIDs:    gids,
	}
	ck.RpcCall("ShardCtrler.Leave", &args, &LeaveReply{})
}
