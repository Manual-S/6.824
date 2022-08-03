package mr

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type Work struct {
	WorkID int // 当前work的id
}

func (w *Work) executeMapTask(reply *TaskReply) error {
	kv := w.getKVByMapF(reply.FileName, reply.mapF)
	if kv == nil {
		return errors.New("kv is nil")
	}
	err := w.writeKVToFile(reply.WorkID, reply.NReduce, kv)
	if err != nil {
		log.Printf("writeKVToFile error err = %v,workID %v", err, reply.WorkID)
		return err
	}
	err := w.notifyTaskFinish(reply.FileID)
	if err != nil {
		log.Printf("notifyTaskFinish error %v")
		return err
	}
	return nil
}

func (w *Work) notifyTaskFinish(fileID int) error {
	request := TaskFinishArgs{}
	request.FileID = fileID
	request.WorkID = w.WorkID

	reply := TaskReply{}
	call("Master.TaskFinish", request, reply)

	if reply.Accept {
		log.Printf("task accept work id %v", w.WorkID)
		return nil
	}
	str := fmt.Sprintf("task accept error work id %v", w.WorkID)
	err := errors.New(str)
	log.Printf("%v", err)

	return err
}

// getKVByMapF 从文件中获取kv键值对
func (w *Work) getKVByMapF(fileName string, mapf func(string, string) []KeyValue) []KeyValue {
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", file)
		return nil
	}

	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", file)
	}
	defer file.Close()

	kva := mapf(fileName, string(content))

	return kva
}

// writeKVToFile todo
func (w *Work) writeKVToFile(workID int, nReduce int, kv []KeyValue) error {
	// 将kv写入到mr-workID-nReduce的文件中
	return nil
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	task := GetTask()
	w := Work{}
	switch task.TaskType {
	case MapTaskType:
		err := w.executeMapTask(task)
		if err != nil {
			log.Printf("executeMapTask error %v", err)
			return
		}
	case ReduceTaskType:

	default:
		panic("Task Type error")
	}
}

// GetTask worker向master获取一个任务
func GetTask() *TaskReply {
	args := ExampleArgs{}
	reply := TaskReply{}
	call("Master.AssignTask", args, reply)

	return &reply
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
