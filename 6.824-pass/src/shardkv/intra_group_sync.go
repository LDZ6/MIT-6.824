package shardkv

// group leader chats with its followers

import (
	t "6.824/pylog/template"
	sc "6.824/shardctrler"
)

const (
	cmdSyncCfg t.OpType = "SyncCfg"
	cmdGcShard t.OpType = "GcShard"
)

type SyncCfgArgs struct {
	t.RpcArgs
	Cfg sc.Config
}

type GcShardArgs struct {
	t.RpcArgs
	KvCfgNum    int
	Shard       int
	EntryCfgNum int
}

func (kv *ShardKV) updateConfigOnce() {
	kv.Lock()
	_, isLeader := kv.Raft().GetState()
	if !isLeader || !kv.canUpdateConfigL() {
		kv.Unlock()
		return
	}
	oldCfgNum := kv.config.Num
	kv.Unlock()

	newCfg := kv.sc.Query(oldCfgNum + 1)
	if newCfg.Num > oldCfgNum {
		kv.syncCfg(newCfg)
	}
}

func (kv *ShardKV) canUpdateConfigL() bool {
	if kv.config.Num == 0 {
		return true
	}
	for shard := 0; shard < sc.NShards; shard += 1 {
		if kv.belongToThisL(shard) {
			if kv.shardArr[shard].Db == nil ||
				kv.shardArr[shard].CfgNum < kv.config.Num {
				return false
			}
		} else if kv.shardArr[shard].Db != nil {
			return false
		}
	}
	return true
}

// synchronize new config to all group members
func (kv *ShardKV) syncCfg(newCfg sc.Config) {
	kv.Lock()
	if kv.config.Num >= newCfg.Num {
		kv.Unlock()
		return
	}
	cmd := t.Op{
		OpType: cmdSyncCfg,
		Args: SyncCfgArgs{
			RpcArgs: kv.GenRpcArgs(false),
			Cfg:     newCfg,
		},
	}
	kv.Unlock()
	kv.RpcExec(cmd)
}

func (kv *ShardKV) doSyncCfgL(newCfg sc.Config) {
	if kv.config.Num != newCfg.Num-1 {
		return
	}

	oldCfg := kv.config
	kv.config = newCfg
	kv.printfL("\nwarning: update config\nold: %+v\nnew: %+v\n"+
		"before update config, shardArr:\n%v",
		oldCfg, newCfg, kv.shardArr)

	for shard := 0; shard < sc.NShards; shard += 1 {
		if !kv.belongToThisL(shard) {
			continue
		}
		if newCfg.Num == 1 {
			kv.shardArr[shard] = ShardEntry{
				Db: map[string]string{
					"-1": "-1", // for debug easier
				},
				CfgNum: 1,
			}
		}
		if kv.shardArr[shard].Db != nil &&
			kv.gid == oldCfg.Shards[shard] &&
			kv.shardArr[shard].CfgNum == oldCfg.Num {
			kv.shardArr[shard].CfgNum = newCfg.Num
		}
	}

	kv.printfL("\nafter update config, shardArr:\n%v", kv.shardArr)
}

func (kv *ShardKV) gcShard(kvCfgNum, shard, entryCfgNum int) {
	kv.Lock()
	if kv.config.Num != kvCfgNum {
		kv.Unlock()
		return
	}
	cmd := t.Op{
		OpType: cmdGcShard,
		Args: GcShardArgs{
			RpcArgs:     kv.GenRpcArgs(false),
			KvCfgNum:    kvCfgNum,
			Shard:       shard,
			EntryCfgNum: entryCfgNum,
		},
	}
	kv.Unlock()
	kv.RpcExec(cmd)
}

func (kv *ShardKV) doGcShardL(kvCfgNum, shard, entryCfgNum int) {
	if kv.config.Num != kvCfgNum {
		return
	}
	if !kv.belongToThisL(shard) &&
		kv.shardArr[shard].CfgNum == entryCfgNum &&
		kv.shardArr[shard].Db != nil {
		kv.shardArr[shard].Db = nil
		kv.printfL("\ngc [shard %v] shardArr:\n%v", shard, kv.shardArr)
	}
}
