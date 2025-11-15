package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"time"

	"6.824/pylog/util"
)

type Coordinator struct {
	mu      util.Mutex
	state   coordState
	tasks   map[int]Task
	nMap    int
	nReduce int
	nTask   int // total num of task be assigned
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	// l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := &Coordinator{
		state:   sMap,
		tasks:   map[int]Task{},
		nMap:    len(files),
		nReduce: nReduce,
		nTask:   0,
	}
	c.server()
	go c.start(files)
	return c
}

func (c *Coordinator) start(files []string) {
	c.mu.Lock()
	printf("--- map phase ---")
	if err := c.createMapTasksL(files); err != nil {
		log.Fatal(err)
	}

	for len(c.tasks) != 0 {
		c.mu.Unlock()
		time.Sleep(time.Second)
		c.mu.Lock()
	}

	printf("--- reduce phase ---")
	c.state = sReduce
	if err := c.createReduceTasksL(files); err != nil {
		log.Fatal(err)
	}

	for len(c.tasks) != 0 {
		c.mu.Unlock()
		time.Sleep(time.Microsecond * 100)
		c.mu.Lock()
	}

	c.state = sDone
	c.mu.Unlock()
	printf("--- MapReduce finish ---")
}

// note. lock before call this func
// make ths last char of func name to be "L"
func (c *Coordinator) createMapTasksL(files []string) (err error) {
	if c.state == sMap {
		for taskID := 0; taskID < c.nMap; taskID += 1 {
			printf("create Map taskID: %v with %v", taskID, files[taskID])
			t := Task{
				TaskID:      taskID,
				State:       sTodo,
				FileNameArr: []string{files[taskID]},
			}
			c.tasks[taskID] = t
		}
		if len(c.tasks) == 0 || len(c.tasks) != c.nMap {
			err = errors.New("create map tasks fail")
		}
	} else {
		err = errors.New("coordState wrong")
	}
	return
}

func (c *Coordinator) createReduceTasksL(files []string) (err error) {
	if c.state == sReduce {
		for taskID := 0; taskID < c.nReduce; taskID += 1 {
			printf("create Reduce taskID: %v", taskID)
			t := Task{
				TaskID:      taskID,
				State:       sTodo,
				FileNameArr: make([]string, 0, c.nMap),
			}
			c.tasks[taskID] = t
		}
		if len(c.tasks) == 0 || len(c.tasks) != c.nReduce {
			err = errors.New("create reduce tasks fail")
		}
	} else {
		err = errors.New("coordState wrong")
	}
	return
}

// Your code here -- RPC handlers for the worker to call.
// args: nothing, reply: a task with coord-info
func (c *Coordinator) AssignTask(args *AskTaskArgs, reply *AskTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	reply.CoordState = c.state
	reply.NReduce = c.nReduce

	for _, t := range c.tasks {
		isTimeOut := (t.State == sDoing) &&
			(time.Since(t.StartTime).Seconds() > 10)
		if t.State == sTodo || isTimeOut {
			c.nTask++
			t.StartTime = time.Now()
			t.State = sDoing
			t.NTask = c.nTask
			c.tasks[t.TaskID] = t
			reply.Task = t
			printf("coord\t %v\t task start\t %v", c.state, t.TaskID)
			break
		}
	}

	return nil
}

// args: a task, reply: nothing
// remove task from task map
func (c *Coordinator) SetTaskDone(args *SetTaskDoneArgs, reply *SetTaskDoneReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	t, exist := c.tasks[args.TaskID]
	if !exist || t.State != sDoing {
		return errors.New("set task done fail: " +
			string(c.state) + " " + strconv.Itoa(args.TaskID))
	}
	delete(c.tasks, t.TaskID)

	printf("coord\t %v\t task finish\t %v", c.state, t.TaskID)
	return nil
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.state == sDone
}
