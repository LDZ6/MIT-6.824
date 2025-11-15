package kvraft

import (
	crand "crypto/rand"
	"fmt"
	"log"
	"math/big"
	mrand "math/rand"
	"time"

	"6.824/labrpc"
	"6.824/pylog/util"
)

type Clerk struct {
	// be careful, index of servers != me of kvservers
	// therefore, I can't use reply.LeaderHint to speed up
	servers     []*labrpc.ClientEnd
	clientId    int64
	sequenceNum int
	leaderId    int
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.clientId = nrand()
	ck.sequenceNum = 0
	ck.randLeader()
	return ck
}

func (ck *Clerk) Get(key string) string {
	ck.sequenceNum += 1
	args := GetArgs{
		Key:         key,
		ClientId:    ck.clientId,
		SequenceNum: ck.sequenceNum,
	}
	ck.printfL("info: Get(key: %v)", key)
	val := ""
	for { // send client request args once
		reply := GetReply{}
		ok := ck.servers[ck.leaderId].Call("KVServer.Get", &args, &reply)
		if !ok {
			ck.randLeader()
			continue
		}
		if reply.State == errWrongLeader {
			ck.nextLeader()
		} else if reply.State == errOk {
			val = reply.Value
			break
		}
		time.Sleep(clientRetryTimeout)
		ck.printfL("retry args: %+v, reply: %+v", args, reply)
	}
	ck.printfL("Get: %+v --> %v | return ", args, val)
	return val
}

func (ck *Clerk) putAppend(key string, value string, isPut bool) {
	ck.sequenceNum += 1
	args := PutAppendArgs{
		ClientId:    ck.clientId,
		SequenceNum: ck.sequenceNum,
		Key:         key,
		Value:       value,
		IsPut:       isPut,
	}
	opType := "Append"
	if isPut {
		opType = "Put"
	}
	ck.printfL("info: %v(key: %v, val: %v)", opType, key, value)
	for { // send client request args once
		reply := PutAppendReply{}
		ok := ck.servers[ck.leaderId].Call("KVServer.PutAppend", &args, &reply)
		if !ok {
			ck.randLeader()
			continue
		}
		if reply.State == errWrongLeader {
			ck.nextLeader()
		} else if reply.State == errOk {
			break
		}
		time.Sleep(clientRetryTimeout)
		ck.printfL("retry args: %+v, reply: %+v", args, reply)
	}
	ck.printfL("%+v | return ", args)
}

func (ck *Clerk) Put(key string, value string) {
	ck.putAppend(key, value, true)
}

func (ck *Clerk) Append(key string, value string) {
	ck.putAppend(key, value, false)
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := crand.Int(crand.Reader, max)
	x := bigx.Int64()
	return x
}

func (ck *Clerk) randLeader() {
	ck.leaderId = mrand.Intn(len(ck.servers))
}

func (ck *Clerk) nextLeader() {
	ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
}

func (ck *Clerk) printfL(format string, a ...interface{}) {
	if util.DebugKvraft {
		info := fmt.Sprintf("{Client %v} leaderId: %v, sequenceNum: %v\n",
			ck.clientId, ck.leaderId, ck.sequenceNum)
		log.Printf(info+format, a...)
	}
}
