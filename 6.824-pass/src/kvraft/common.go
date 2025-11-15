package kvraft

import (
	"time"
)

type opType string
type err string

const (
	cmdGet    opType = "Get"
	cmdPut    opType = "Put"
	cmdAppend opType = "Append"

	errOk           err = "OK"
	errWrongLeader  err = "ErrWrongLeader"
	errApplyTimeOut err = "ErrApplyTimeOut"

	clientRetryTimeout time.Duration = time.Millisecond *
		time.Duration(100)
	serverWaitApplyTimeout time.Duration = time.Millisecond *
		time.Duration(750)
)

type GetArgs struct {
	ClientId    int64
	SequenceNum int
	Key         string
}

type GetReply struct {
	State err
	Value string
}

type PutAppendArgs struct {
	ClientId    int64
	SequenceNum int
	Key         string
	Value       string
	IsPut       bool
}

type PutAppendReply struct {
	State err
}
