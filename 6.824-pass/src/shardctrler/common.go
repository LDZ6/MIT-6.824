package shardctrler

import (
	"6.824/labgob"
	t "6.824/pylog/template"
)

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // [shard] -> gid
	Groups map[int][]string // [gid] -> servers[]
}

const (
	cmdQuery t.OpType = "Query"
	cmdMove  t.OpType = "Move"
	cmdJoin  t.OpType = "Join"
	cmdLeave t.OpType = "Leave"
)

type QueryArgs struct {
	t.RpcArgs
	Num int // desired config number
}

type QueryReply struct {
	t.RpcReply
	Config Config
}

type MoveArgs struct {
	t.RpcArgs
	Shard int
	GID   int
}

type MoveReply struct {
	t.RpcReply
}

type JoinArgs struct {
	t.RpcArgs
	Servers map[int][]string // new [gid] -> servers
}

type JoinReply struct {
	t.RpcReply
}

type LeaveArgs struct {
	t.RpcArgs
	GIDs []int
}

type LeaveReply struct {
	t.RpcReply
}

func register(isServer bool) {
	t.Register(isServer)
	labgob.Register(Config{})
	labgob.Register(QueryArgs{})
	labgob.Register(QueryReply{})
	labgob.Register(MoveArgs{})
	labgob.Register(MoveReply{})
	labgob.Register(JoinArgs{})
	labgob.Register(JoinReply{})
	labgob.Register(LeaveArgs{})
	labgob.Register(LeaveReply{})
}
