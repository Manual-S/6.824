package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
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

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// executeMapTask 处理MapTask
func (w *Work) executeMapTask(reply *TaskReply, mapF func(string, string) []KeyValue) error {
	kv, err := w.getKVByMapF(reply.FileName, mapF)
	if err != nil {
		log.Printf("getKVByMapF error %v", err)
		return err
	}
	err = w.writeKVToFile(reply.WorkID, reply.NReduce, kv)
	if err != nil {
		log.Printf("writeKVToFile error err = %v,workID %v", err, reply.WorkID)
		return err
	}
	err = w.notifyMapTaskFinish(reply.FileID)
	if err != nil {
		log.Printf("notifyTaskFinish error %v")
		return err
	}
	return nil
}

func (w *Work) notifyMapTaskFinish(fileID int) error {
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
func (w *Work) getKVByMapF(fileName string, mapf func(string, string) []KeyValue) ([]KeyValue, error) {
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", file)
		return nil, err
	}

	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", file)
		return nil, err
	}
	defer file.Close()

	kva := mapf(fileName, string(content))

	return kva, nil
}

// writeKVToFile todo
func (w *Work) writeKVToFile(workID int, nReduce int, kv []KeyValue) error {
	// 将kv写入到mr-workID-nReduce的文件中
	return nil
}

func (w *Work) executeReduceTask(reply *TaskReply) error {
	// 读取map task产生的键值对文件
	intermediate := make([]KeyValue, 0)
	for i := 0; i < reply.FileCount; i++ {
		temp, err := w.readIntermediateFile(i, reply.ReduceIndex)
		if err != nil {
			log.Printf("readIntermediateFile error,wordID = %v err = %v", w.WorkID, err)
			return err
		}
		intermediate = append(intermediate, temp...)
	}
	// 对键值对文件进行排序 将文件写入到最终输出里
	tempFile, err := os.CreateTemp(".", "mrTemp")

	defer tempFile.Close()

	if err != nil {
		log.Printf("CreateTemp error %v", err)
		return err
	}

	// 重命名文件
	outName := fmt.Sprintf("mr-out-%s", reply.ReduceIndex)
	err = os.Rename(tempFile.Name(), outName)
	if err != nil {
		log.Printf("Rename error %v", err)
		return err
	}
	// 上报任务完成

	return nil
}

// 上报reduce task完成
func (w *Work) notifyReduceTaskFinish() error {
	return nil
}

func (w *Work) reduceKVToTempFile(intermediate []KeyValue, reducef func(string, []string) string, tempFile *os.File) error {
	sort.Sort(ByKey(intermediate))

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	return nil
}

func (w *Work) readIntermediateFile(fileID int, reduceID int) ([]KeyValue, error) {
	res := make([]KeyValue, 0)
	fileName := fmt.Sprintf("mr-%s-%s", fileID, reduceID)
	file, err := os.Open(fileName)

	defer file.Close()

	if err != nil {
		log.Printf("Open file error,workID = %v,err = %v", w.WorkID, err)
		return nil, err
	}
	dec := json.NewDecoder(file)

	for {
		var t KeyValue
		err := dec.Decode(&t)
		if err != nil {
			// todo 这里代码要改动一下
			break
		}
		res = append(res, t)
	}

	return res, nil
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
		err := w.executeMapTask(task, mapf)
		if err != nil {
			log.Printf("executeMapTask error %v", err)
			return
		}
	case ReduceTaskType:
		err := w.executeReduceTask(task)
		if err != nil {
			log.Printf("executeReduceTask error %v", err)
			return
		}
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
