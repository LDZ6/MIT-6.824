package kvraft_impl2

import (
	t "6.824/pylog/template"
)

func (kv *KVServer) ExecApplyCmdL(cmd t.Op) t.Err {
	switch cmd.OpType {
	case cmdGet:
		// read only
	case cmdPut:
		fallthrough
	case cmdAppend:
		args := cmd.Args.(PutAppendArgs)
		kv.doPutAppendL(args.Key, args.Value, args.IsPut)
	}
	kv.printfL("exec %+v", cmd)
	return t.OK
}

func (kv *KVServer) doGet(key string) string {
	kv.Lock()
	defer kv.Unlock()
	val, exist := kv.db[key]
	if exist {
		return val
	}
	return ""
}

func (kv *KVServer) doPutAppendL(key, val string, isPut bool) {
	if isPut {
		kv.db[key] = val
	} else {
		kv.db[key] += val
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	cmd := t.Op{
		OpType: cmdGet,
		Args:   *args,
	}
	reply.State = kv.RpcExec(cmd)
	if reply.State == t.OK {
		reply.Value = kv.doGet(args.Key)
	} else {
		reply.Value = ""
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	cmd := t.Op{
		OpType: cmdAppend,
		Args:   *args,
	}
	if args.IsPut {
		cmd.OpType = cmdPut
	}
	reply.State = kv.RpcExec(cmd)
}
