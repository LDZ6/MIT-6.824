package template

import (
	crand "crypto/rand"
	"math/big"
	mrand "math/rand"
	"time"

	"6.824/labrpc"
	"6.824/pylog/util"
)

type Clerk struct {
	mu          util.Mutex
	servers     []*labrpc.ClientEnd
	clientId    int64
	sequenceNum int
	gid         int         // servers/raft group id
	leader      map[int]int // [gid] -> leaderHint
}

func (ck *Clerk) Init(servers []*labrpc.ClientEnd) {
	ck.servers = servers
	ck.clientId = nrand()
	ck.sequenceNum = 0
	ck.leader = map[int]int{}
	// one group in lab 3 and 4A, multiple groups in lab 4B
	ck.AssignGroup(0, servers)
}

// for lab 4B, which needs to send rpc to diffrent groups
func (ck *Clerk) AssignGroup(gid int, servers []*labrpc.ClientEnd) {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	ck.gid = gid
	ck.servers = servers
}

func (ck *Clerk) GenRpcArgs(keepOrder bool) RpcArgs {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	ck.sequenceNum += 1
	return RpcArgs{
		ClientId:    ck.clientId,
		SequenceNum: ck.sequenceNum,
		KeepOrder:   keepOrder,
	}
}

func (ck *Clerk) RpcCall(funcName string, argsPtr ArgsI, emptyReplyPtr ReplyI) ReplyI {
	ck.mu.Lock()
	length := len(ck.servers)
	if length < 1 {
		ck.mu.Unlock()
		return nil
	}
	leader, exist := ck.leader[ck.gid]
	if !exist || leader < 0 || leader >= length {
		ck.leader[ck.gid] = mrand.Intn(length) // rand one
	}

	for { // send client request args once
		replyPtr := emptyReplyPtr
		clinetEnd := ck.servers[ck.leader[ck.gid]]
		ck.mu.Unlock()
		ok := clinetEnd.Call(funcName, argsPtr, replyPtr)
		if !ok {
			ck.mu.Lock()
			ck.leader[ck.gid] = mrand.Intn(length) // rand one
			continue
		}
		state := replyPtr.Common().State
		if state != errWrongLeader && state != errApplyTimeOut {
			return replyPtr // OK, or undefined Err in template
		}
		// retry
		if state == errWrongLeader {
			ck.mu.Lock()
			ck.leader[ck.gid] = (ck.leader[ck.gid] + 1) % length // next one
			ck.mu.Unlock()
		}
		time.Sleep(clientRetryInterval)
		ck.mu.Lock()
	}
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := crand.Int(crand.Reader, max)
	x := bigx.Int64()
	return x
}
