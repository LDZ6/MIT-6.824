package mr

import (
	"fmt"
	"log"
	"net/rpc"
	"os"
	"strconv"
	"time"

	"6.824/pylog/util"
)

type coordState string
type taskState string

const (
	sMap    coordState = "Map"
	sReduce coordState = "Reduce"
	sDone   coordState = "Done"

	sTodo  taskState = "Todo"
	sDoing taskState = "Doing"
)

type Task struct {
	NTask       int // for create tmp file
	TaskID      int
	State       taskState
	FileNameArr []string
	StartTime   time.Time
}

// Add your RPC definitions here.
type AskTaskArgs struct {
}

type AskTaskReply struct {
	CoordState coordState
	Task       Task
	NReduce    int
}

type SetTaskDoneArgs struct {
	TaskID int
}

type SetTaskDoneReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		// log.Fatal("dialing:", err)
		os.Exit(0)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
	}
	return err == nil
}

func printf(format string, a ...interface{}) {
	if util.DebugMapReduce {
		log.Printf(format, a...)
	}
}
