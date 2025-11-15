package kvraft_impl2

import (
	"6.824/labgob"
	t "6.824/pylog/template"
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
	labgob.Register(GetArgs{})
	labgob.Register(GetReply{})
	labgob.Register(PutAppendArgs{})
	labgob.Register(PutAppendReply{})
}
