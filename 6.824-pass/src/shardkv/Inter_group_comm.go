package shardkv

// group leader chats with another group

import (
	"sync/atomic"
	"time"

	t "6.824/pylog/template"
	"6.824/pylog/util"
	sc "6.824/shardctrler"
)

const (
	cmdMigrateShard t.OpType = "MigrateShard"

	connectOtherGroupTimeout time.Duration = time.Second *
		time.Duration(5)
	pushShardInterval time.Duration = time.Millisecond *
		time.Duration(50)
)

type MigrateShardArgs struct {
	t.RpcArgs
	PusherKvCfgNum int
	Shard          int
	Entry          ShardEntry
	Session        map[int64]t.SessionEntry
}

type MigrateShardReply struct {
	t.RpcReply
}

func (kv *ShardKV) pushShardOnce() bool {
	kv.Lock()
	_, isLeader := kv.Raft().GetState()
	if !isLeader {
		kv.Unlock()
		return false
	}

	kvCfgNum := kv.config.Num
	if kvCfgNum == 0 {
		kv.Unlock()
		return false
	}

	shard := 0
	entry := ShardEntry{}
	for ; shard < sc.NShards; shard += 1 {
		if kv.shardArr[shard].Db != nil && !kv.belongToThisL(shard) &&
			kv.shardArr[shard].CfgNum <= kvCfgNum {
			entry = kv.shardArr[shard]
			break
		}
	}
	kv.Unlock()
	if shard == sc.NShards {
		return false
	}

	if entry.CfgNum == 0 {
		kv.gcShard(kvCfgNum, shard, entry.CfgNum)
		return true
	}

	var connectOk int32 = 0
	var needGc int32 = 0
	go func() {
		if kv.ckMigrateShard(kvCfgNum, shard, entry) {
			atomic.StoreInt32(&needGc, 1)
		}
		atomic.StoreInt32(&connectOk, 1)
	}()

	// timeout when the target group is killed
	t0 := time.Now()
	for time.Since(t0) < connectOtherGroupTimeout {
		if atomic.LoadInt32(&connectOk) == 1 {
			break
		}
	}
	if atomic.LoadInt32(&connectOk) == 0 {
		kv.Lock()
		kv.printfL("warning: push shard %v time out, shardArr:\n%v", shard, kv.shardArr)
		kv.Unlock()
		return false
	}
	if atomic.LoadInt32(&needGc) == 1 {
		kv.gcShard(kvCfgNum, shard, entry.CfgNum)
	}
	return true
}

// as clerk, push shard to the group it belongs to, return if success
func (kv *ShardKV) ckMigrateShard(kvCfgNum, shard int, entry ShardEntry) bool {
	kv.Lock()
	if kvCfgNum == 0 || kvCfgNum != kv.config.Num {
		kv.Unlock()
		return false
	}

	gid := kv.config.Shards[shard]
	servers := targetGroupL(gid, kv.config.Groups, kv.make_end)
	if servers == nil {
		kv.printfL("warning: make_end nil, gid %v", gid)
		kv.Unlock()
		return false
	}
	kv.AssignGroup(gid, servers)

	args := MigrateShardArgs{
		RpcArgs:        kv.GenRpcArgs(false),
		PusherKvCfgNum: kvCfgNum,
		Shard:          shard,
		Session:        kv.CloneSessionL(),
		Entry:          entry.clone(),
	}

	kv.printfL("\ntry send [shard %v] to gid %v, shard: %v",
		shard, gid, kv.shardArr[shard])
	kv.Unlock()

	reply := kv.RpcCall("ShardKV.SvMigrateShard",
		&args, &MigrateShardReply{}).Common()

	kv.Lock()
	kv.printfL("\nsend [shard %v] to gid %v with state: %v, shardArr:\n%v",
		shard, gid, reply.State, kv.shardArr)
	kv.Unlock()
	return reply.State == t.OK
}

// as server, recive the pushed shard
func (kv *ShardKV) SvMigrateShard(args *MigrateShardArgs, reply *MigrateShardReply) {
	reply.State = errWrongGroup
	kv.Lock()
	if kv.config.Num < args.PusherKvCfgNum {
		kv.printfL("\nsv migrate shard %v return early", args.Shard)
		kv.Unlock()
		return
	}

	if kv.shardArr[args.Shard].Db != nil &&
		kv.shardArr[args.Shard].CfgNum >= args.Entry.CfgNum {
		kv.printfL("\nwarning: duplicate [shard %v] before start\nargs: %v\nself: %v",
			args.Shard, args.Entry, kv.shardArr[args.Shard])
		kv.Unlock()
		reply.State = t.OK
		return
	}

	kv.printfL("\nsv migrate shard %v call rpc exec", args.Shard)
	cmd := t.Op{
		OpType: cmdMigrateShard,
		Args:   *args,
	}
	kv.Unlock()
	reply.State = kv.RpcExec(cmd)
}

func (kv *ShardKV) doMigrateShardL(pusherKvCfgNum, shard int, entry ShardEntry,
	session map[int64]t.SessionEntry) t.Err {
	if kv.config.Num < pusherKvCfgNum {
		kv.printfL("\ndo migrate shard %v return early", shard)
		return errWrongGroup
	}

	kv.MergeSessionL(session)
	if kv.shardArr[shard].CfgNum >= entry.CfgNum {
		kv.printfL("\nduplicate [shard %v] without clone\nargs: %v\nself: %v",
			shard, entry, kv.shardArr[shard])
	} else {
		entry.CfgNum = util.MaxInt(entry.CfgNum, kv.config.Num)
		kv.shardArr[shard] = entry.clone()
		kv.printfL("\nrecive [shard %v cfg %v] shardArr:\n%v",
			shard, entry.CfgNum, kv.shardArr)
	}

	return t.OK
}
