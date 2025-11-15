package shardctrler

import (
	"sort"

	t "6.824/pylog/template"
)

func (sc *ShardCtrler) ExecApplyCmdL(cmd t.Op) t.Err {
	switch cmd.OpType {
	case cmdQuery:
		// read only
	case cmdMove:
		args := cmd.Args.(MoveArgs)
		sc.doMoveL(args.Shard, args.GID)
	case cmdJoin:
		args := cmd.Args.(JoinArgs)
		sc.doJoinL(args.Servers)
	case cmdLeave:
		args := cmd.Args.(LeaveArgs)
		sc.doLeaveL(args.GIDs)
	}

	if cmd.OpType != cmdQuery {
		sc.printfL("exec %+v", cmd)
	}
	return t.OK
}

func (sc *ShardCtrler) initNextConfigL() Config {
	lastCfg := sc.doQueryL(-1)
	newCfg := Config{
		Num:    lastCfg.Num + 1,
		Shards: lastCfg.Shards,
		Groups: map[int][]string{},
	}
	// map must be deep copied
	for gid, servers := range lastCfg.Groups {
		newCfg.Groups[gid] = servers
	}
	return newCfg
}

// return map[gid] -> arr of shards, based arr[shard] -> gid
func gidToShards(cfg Config) map[int][]int {
	g2s := make(map[int][]int)
	for gid := range cfg.Groups {
		g2s[gid] = []int{}
	}
	for shard, gid := range cfg.Shards {
		g2s[gid] = append(g2s[gid], shard)
	}
	return g2s
}

// return arr[shard] -> gid, based map[gid] -> shard arr
func shardToGid(gidToShardArr map[int][]int) [NShards]int {
	s2g := [NShards]int{}
	for gid, shardArr := range gidToShardArr {
		for _, shard := range shardArr {
			s2g[shard] = gid
		}
	}
	return s2g
}

func groupWithLeastShard(gidToShardArr map[int][]int) int {
	gidArr := []int{}
	for gid := range gidToShardArr {
		gidArr = append(gidArr, gid)
	}
	sort.Ints(gidArr)

	minCnt, minIdx := NShards+1, -1
	for _, gid := range gidArr {
		if gid != 0 && len(gidToShardArr[gid]) < minCnt {
			minCnt = len(gidToShardArr[gid])
			minIdx = gid
		}
	}
	return minIdx
}

func groupWithMostShard(gidToShardArr map[int][]int) int {
	if shardArr, ok := gidToShardArr[0]; ok && len(shardArr) > 0 {
		return 0
	}

	gidArr := []int{}
	for gid := range gidToShardArr {
		gidArr = append(gidArr, gid)
	}
	sort.Ints(gidArr)

	maxCnt, maxIdx := -1, -1
	for _, gid := range gidArr {
		if len(gidToShardArr[gid]) > maxCnt {
			maxCnt = len(gidToShardArr[gid])
			maxIdx = gid
		}
	}
	return maxIdx
}

func (sc *ShardCtrler) doQueryL(num int) Config {
	length := len(sc.configs)
	if num < 0 || num >= length {
		num = length - 1
	}
	return sc.configs[num]
}

func (sc *ShardCtrler) doMoveL(shard, gid int) {
	newCfg := sc.initNextConfigL()
	newCfg.Shards[shard] = gid
	sc.configs = append(sc.configs, newCfg)
}

func (sc *ShardCtrler) doJoinL(servers map[int][]string) {
	newCfg := sc.initNextConfigL()
	for gid, s := range servers { // join
		newCfg.Groups[gid] = s
	}

	g2s := gidToShards(newCfg)
	for { // move one shard of the most into the least
		src, dst := groupWithMostShard(g2s), groupWithLeastShard(g2s)
		if src != 0 && len(g2s[src])-len(g2s[dst]) <= 1 {
			break
		}
		g2s[dst] = append(g2s[dst], g2s[src][0])
		g2s[src] = g2s[src][1:]
	}
	newCfg.Shards = shardToGid(g2s)

	sc.configs = append(sc.configs, newCfg)
}

func (sc *ShardCtrler) doLeaveL(gidArr []int) {
	newCfg := sc.initNextConfigL()
	g2s := gidToShards(newCfg)
	freeShards := make([]int, 0)
	for _, gid := range gidArr { // leave
		delete(newCfg.Groups, gid)
		if shards, ok := g2s[gid]; ok {
			freeShards = append(freeShards, shards...)
			delete(g2s, gid)
		}
	}

	if len(newCfg.Groups) > 0 {
		// move one free shard into the least
		for _, shard := range freeShards {
			dst := groupWithLeastShard(g2s)
			g2s[dst] = append(g2s[dst], shard)
		}
		newCfg.Shards = shardToGid(g2s)
	} else { // if all group leaved, no shard to store
		newCfg.Shards = [NShards]int{}
	}

	sc.configs = append(sc.configs, newCfg)
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	cmd := t.Op{
		OpType: cmdQuery,
		Args:   *args,
	}
	reply.State = sc.RpcExec(cmd)
	if reply.State == t.OK {
		sc.Lock()
		reply.Config = sc.doQueryL(args.Num)
		sc.Unlock()
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	cmd := t.Op{
		OpType: cmdMove,
		Args:   *args,
	}
	reply.State = sc.RpcExec(cmd)
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	cmd := t.Op{
		OpType: cmdJoin,
		Args:   *args,
	}
	reply.State = sc.RpcExec(cmd)
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	cmd := t.Op{
		OpType: cmdLeave,
		Args:   *args,
	}
	reply.State = sc.RpcExec(cmd)
}
