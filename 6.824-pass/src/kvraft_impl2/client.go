package kvraft_impl2

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

func (ck *Clerk) Get(key string) string {
	args := GetArgs{
		RpcArgs: ck.GenRpcArgs(true),
		Key:     key,
	}
	reply := ck.RpcCall("KVServer.Get", &args, &GetReply{}).(*GetReply)
	return reply.Value
}

func (ck *Clerk) putAppend(key string, value string, isPut bool) {
	args := PutAppendArgs{
		RpcArgs: ck.GenRpcArgs(true),
		Key:     key,
		Value:   value,
		IsPut:   isPut,
	}
	ck.RpcCall("KVServer.PutAppend", &args, &GetReply{})
}

func (ck *Clerk) Put(key string, value string) {
	ck.putAppend(key, value, true)
}

func (ck *Clerk) Append(key string, value string) {
	ck.putAppend(key, value, false)
}
