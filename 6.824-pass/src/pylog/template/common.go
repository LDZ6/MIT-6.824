package template

import (
	"time"

	"6.824/labgob"
)

const (
	clientRetryInterval time.Duration = time.Millisecond *
		time.Duration(100)
	serverWaitApplyTimeout time.Duration = time.Millisecond *
		time.Duration(750)
)

type Err string

const (
	OK              Err = "OK"
	errWrongLeader  Err = "ErrWrongLeader"
	errApplyTimeOut Err = "ErrApplyTimeOut"
)

type OpType string

// command of raft's log entry
type Op struct {
	OpType OpType
	Args   ArgsI
}

type RpcArgs struct {
	ClientId    int64
	SequenceNum int
	KeepOrder   bool
}

type RpcReply struct {
	State Err
}

type ArgsI interface {
	Common() RpcArgs
}

type ReplyI interface {
	Common() RpcReply
}

func (args RpcArgs) Common() RpcArgs {
	return args
}

func (reply RpcReply) Common() RpcReply {
	return reply
}

func Register(isServer bool) {
	if isServer {
		labgob.Register(Op{})
		labgob.Register(SessionEntry{})
	}
	labgob.Register(RpcArgs{})
	labgob.Register(RpcReply{})
}
