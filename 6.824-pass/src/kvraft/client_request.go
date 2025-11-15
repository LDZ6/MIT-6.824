package kvraft

import "time"

func (kv *KVServer) execApplyCmdL(cmd Op) err {
	kv.printfL("exec %+v", cmd)
	switch cmd.OpType {
	case cmdGet:
		// read only
	case cmdPut:
		kv.db[cmd.Key] = cmd.Value
	case cmdAppend:
		kv.db[cmd.Key] += cmd.Value
	}
	return errOk
}

func (kv *KVServer) updateSessionL(state err,
	clientId int64, sequenceNum int) {
	if state == errOk {
		kv.session[clientId] = sequenceNum
	}
}

func (kv *KVServer) searchSessionL(clientId int64,
	sequenceNum int) (exist bool) {
	entry, exist := kv.session[clientId]
	if exist && entry >= sequenceNum {
		exist = true
	} else {
		exist = false
	}
	return
}

func (kv *KVServer) waitApply(clientId int64,
	sequenceNum int) (exits bool) {
	t0 := time.Now()
	for !kv.killed() && time.Since(t0) < serverWaitApplyTimeout {
		kv.mu.Lock()
		exits = kv.searchSessionL(clientId, sequenceNum)
		kv.mu.Unlock()
		if exits {
			return
		}
	}
	return
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	reply.State = errWrongLeader
	reply.Value = ""
	kv.mu.Lock()
	exist := kv.searchSessionL(args.ClientId, args.SequenceNum)
	if exist {
		reply.State = errOk
		val, exist := kv.db[args.Key]
		if exist {
			reply.Value = val
		}
		kv.printfL("duplicate cmd, before start, seq: %v", args.SequenceNum)
		kv.mu.Unlock()
		return
	}

	cmd := Op{
		OpType:      cmdGet,
		ClientId:    args.ClientId,
		SequenceNum: args.SequenceNum,
		Key:         args.Key,
		Value:       "",
	}
	_, _, isLeader := kv.rf.Start(cmd)
	kv.printfL("start cmd: %+v", cmd)
	kv.mu.Unlock()

	if !isLeader {
		kv.printfL("isn't leader")
		return
	}

	exist = kv.waitApply(args.ClientId, args.SequenceNum)
	if !exist {
		reply.State = errApplyTimeOut
		kv.printfL("raft apply timeout | %v", cmd)
	} else {
		reply.State = errOk
		kv.mu.Lock()
		val, exist := kv.db[args.Key]
		if exist {
			reply.Value = val
		}
		kv.mu.Unlock()
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	reply.State = errWrongLeader
	kv.mu.Lock()
	exist := kv.searchSessionL(args.ClientId, args.SequenceNum)
	if exist {
		reply.State = errOk
		kv.printfL("duplicate cmd, before start, seq: %v", args.SequenceNum)
		kv.mu.Unlock()
		return
	}

	cmd := Op{
		OpType:      cmdAppend,
		ClientId:    args.ClientId,
		SequenceNum: args.SequenceNum,
		Key:         args.Key,
		Value:       args.Value,
	}
	if args.IsPut {
		cmd.OpType = cmdPut
	}
	_, _, isLeader := kv.rf.Start(cmd)
	kv.printfL("start cmd: %+v", cmd)
	kv.mu.Unlock()

	if !isLeader {
		kv.printfL("isn't leader")
		return
	}

	exist = kv.waitApply(args.ClientId, args.SequenceNum)
	if !exist {
		reply.State = errApplyTimeOut
		kv.printfL("raft apply timeout | %v", cmd)
	} else {
		reply.State = errOk
	}
}
