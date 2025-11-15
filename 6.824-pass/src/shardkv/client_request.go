package shardkv

import (
	t "6.824/pylog/template"
)

func (kv *ShardKV) ExecApplyCmdL(cmd t.Op) (state t.Err) {
	state = t.OK
	switch cmd.OpType {
	// for client
	case cmdGet:
		// read only
	case cmdPut:
		fallthrough
	case cmdAppend:
		args := cmd.Args.(PutAppendArgs)
		state = kv.doPutAppendL(args.Key, args.Value, args.IsPut)
		// for server
	case cmdMigrateShard:
		args := cmd.Args.(MigrateShardArgs)
		state = kv.doMigrateShardL(args.PusherKvCfgNum,
			args.Shard, args.Entry, args.Session)
	case cmdSyncCfg:
		args := cmd.Args.(SyncCfgArgs)
		kv.doSyncCfgL(args.Cfg)
	case cmdGcShard:
		args := cmd.Args.(GcShardArgs)
		kv.doGcShardL(args.KvCfgNum, args.Shard, args.EntryCfgNum)
	}
	kv.printfL("exec %+v, with state: %v", cmd, state)
	return
}

func (kv *ShardKV) checkReadyL(shard int) t.Err {
	if !kv.belongToThisL(shard) {
		return errWrongGroup
	}
	if kv.shardArr[shard].Db != nil &&
		kv.shardArr[shard].CfgNum == kv.config.Num {
		return t.OK
	} else {
		return errShardNotReady
	}
}

func (kv *ShardKV) doGet(key string) (value string, state t.Err) {
	kv.Lock()
	defer kv.Unlock()
	shard := key2shard(key)
	state = kv.checkReadyL(shard)
	if state == t.OK {
		value = kv.shardArr[shard].Db[key]
	}
	return
}

func (kv *ShardKV) doPutAppendL(key, value string, isPut bool) (state t.Err) {
	shard := key2shard(key)
	state = kv.checkReadyL(shard)
	if state == t.OK {
		if isPut {
			kv.shardArr[shard].Db[key] = value
		} else {
			kv.shardArr[shard].Db[key] += value
		}
	}
	return
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	shard := key2shard(args.Key)
	kv.Lock()
	reply.State = kv.checkReadyL(shard)
	kv.Unlock()
	if reply.State != t.OK {
		return
	}
	cmd := t.Op{
		OpType: cmdGet,
		Args:   *args,
	}
	reply.State = kv.RpcExec(cmd)
	if reply.State == t.OK {
		reply.Value, reply.State = kv.doGet(args.Key)
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	shard := key2shard(args.Key)
	kv.Lock()
	reply.State = kv.checkReadyL(shard)
	kv.Unlock()
	if reply.State != t.OK {
		return
	}
	cmd := t.Op{
		OpType: cmdAppend,
		Args:   *args,
	}
	if args.IsPut {
		cmd.OpType = cmdPut
	}
	reply.State = kv.RpcExec(cmd)
}
