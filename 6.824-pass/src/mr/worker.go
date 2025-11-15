package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(
	mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	printf("worker start")

	for {
		coordState, nReduce, task, ok := askTask()
		if !ok {
			time.Sleep(time.Microsecond * 100)
			continue
		}

		printf("worker\t %v\t task start\t %v", coordState, task.TaskID)
		if coordState == sMap {
			ok = doMap(task, nReduce, mapf)
		} else if coordState == sReduce {
			ok = doReduce(task, reducef)
		} else if coordState == sDone {
			break
		}

		if ok && finishTask(task) {
			printf("worker\t %v\t task finish\t %v", coordState, task.TaskID)
		}
	}
}

func askTask() (coordState coordState, nReduce int, task Task, ok bool) {
	reply := AskTaskReply{}
	rpcOk := call("Coordinator.AssignTask", &AskTaskArgs{}, &reply)
	if rpcOk {
		coordState = reply.CoordState
		nReduce = reply.NReduce
		task = reply.Task
		ok = task.NTask != 0
	}
	return
}

func finishTask(task Task) bool {
	args := SetTaskDoneArgs{
		TaskID: task.TaskID,
	}
	rpcOk := call("Coordinator.SetTaskDone", &args, &SetTaskDoneReply{})
	return rpcOk
}

func doMap(task Task, nReduce int,
	mapf func(string, string) []KeyValue) bool {
	if len(task.FileNameArr) != 1 {
		return false
	}
	key := task.FileNameArr[0]
	value := readText(key)
	if value == "" {
		return false
	}
	kva := mapf(key, value)

	var jsonNameMap = make(map[int]string, nReduce)
	var jsonName string = ""
	jsonNameTmp := "mrtmp" + strconv.Itoa(task.NTask) +
		"-" + strconv.Itoa(task.TaskID) + "-"

	for _, kv := range kva {
		reduceID := ihash(kv.Key) % nReduce
		jsonName = jsonNameTmp + strconv.Itoa(reduceID) + ".json"
		if appendJson(jsonName, kv) {
			jsonNameMap[reduceID] = jsonName
		} else {
			return false
		}
	}

	// must not shuffle here, becase of the mrapps, such as wc,
	// which not supports to reduce shuffled-intermeadiate-file
	ok := true
	for _, oldName := range jsonNameMap {
		newName := "mr" + oldName[5+len(strconv.Itoa(task.NTask)):]
		ok = rename(oldName, newName)
	}
	return ok
}

func doReduce(task Task,
	reducef func(string, []string) string) bool {
	// load intermediate files at task.Filename first
	pwd, _ := os.Getwd()
	filter := "mr-*-" + strconv.Itoa(task.TaskID) + ".json"
	fileNameArr, err := filepath.Glob(filepath.Join(pwd, filter))
	if err != nil {
		fmt.Fprintln(os.Stderr, "get json name error", err)
		return false
	}
	task.FileNameArr = append(task.FileNameArr, fileNameArr...)

	// load intermediate to memory with sorting
	intermediate := []KeyValue{}
	for _, fn := range task.FileNameArr {
		kva := readJson(fn)
		if kva == nil {
			return false
		}
		intermediate = append(intermediate, kva...)
	}
	sort.Sort(ByKey(intermediate))

	// call reducef and write its return
	textName := "mrtmp" + strconv.Itoa(task.NTask) +
		"-out-" + strconv.Itoa(task.TaskID) + ".txt"
	for i, j := 0, 1; i < len(intermediate); i = j {
		for j < len(intermediate) &&
			intermediate[j].Key == intermediate[i].Key {
			j++
		}
		if len(intermediate[i].Key) == 0 {
			continue // skip blank
		}
		values := []string{}
		for k := i; k < j; k += 1 {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		if !appendText(textName, intermediate[i].Key+" "+output) {
			return false
		}
	}

	ok := true
	if isFileExist(textName) {
		newName := "mr" + textName[5+len(strconv.Itoa(task.NTask)):]
		ok = rename(textName, newName)
	}
	return ok
}

func isFileExist(filename string) bool {
	_, err := os.Stat(filename)
	if err == nil {
		return true
	}
	if os.IsNotExist(err) {
		return false
	} else {
		fmt.Fprintln(os.Stderr, "At isFileExist:", err)
		return false
	}
}

func rename(oldFileName, newFileName string) bool {
	err := os.Rename(oldFileName, newFileName)
	if err != nil {
		fmt.Fprintln(os.Stderr, "At rename:", err)
		return false
	}
	return true
}

func readText(filename string) string {
	file, err := os.Open(filename)
	if err != nil {
		fmt.Fprintln(os.Stderr, "At read text:", err)
		return ""
	}
	defer file.Close()

	content, err := ioutil.ReadAll(file)
	if err != nil {
		fmt.Fprintln(os.Stderr, "At read text:", err)
		return ""
	}
	return string(content)
}

func appendText(filename string, newLine string) bool {
	file, err := os.OpenFile(filename,
		os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0777)
	if err != nil {
		fmt.Fprintln(os.Stderr, "At append text:", err)
		return false
	}
	defer file.Close()

	_, err = fmt.Fprintf(file, newLine+"\n")
	if err != nil {
		fmt.Fprintln(os.Stderr, "At append text:", err)
		return false
	}
	return true
}

func readJson(filename string) []KeyValue {
	file, err := os.Open(filename)
	if err != nil {
		fmt.Fprintln(os.Stderr, "At read json:", err)
		return nil
	}
	defer file.Close()

	dec := json.NewDecoder(file)
	kva := []KeyValue{}
	for dec.More() {
		var kv KeyValue
		err := dec.Decode(&kv)
		if err != nil {
			fmt.Fprintln(os.Stderr, "At read json:", err)
			return nil
		}
		kva = append(kva, kv)
	}
	return kva
}

func appendJson(filename string, kv KeyValue) bool {
	file, err := os.OpenFile(filename,
		os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0777)
	if err != nil {
		fmt.Fprintln(os.Stderr, "At append json:", err)
		return false
	}
	defer file.Close()

	jsonbyte, err := json.Marshal(kv)
	if err != nil {
		fmt.Fprintln(os.Stderr, "json convert to kv error:", err)
		return false
	}
	_, err = file.WriteString(string(jsonbyte) + "\n")
	if err != nil {
		fmt.Fprintln(os.Stderr, "At append json:", err)
		return false
	}
	return true
}
