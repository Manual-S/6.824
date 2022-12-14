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

	log.Printf("executeMapTask fileID is %v", reply.FileID)

	kv, err := w.getKVByMapF(reply.FileName, mapF)
	if err != nil {
		log.Printf("getKVByMapF error %v", err)
		return err
	}

	log.Printf("finish getKVByMapF")

	err = w.writeKVToFile(reply.FileID, reply.NReduce, kv)
	if err != nil {
		log.Printf("writeKVToFile error err = %v,workID %v", err, reply.WorkID)
		return err
	}

	log.Printf("finish writeKVToFile")

	err = w.notifyMapTaskFinish(reply.FileID)
	if err != nil {
		log.Printf("notifyTaskFinish error %v")
		return err
	}
	return nil
}

// notifyMapTaskFinish
func (w *Work) notifyMapTaskFinish(fileID int) error {
	request := TaskFinishArgs{}
	request.FileID = fileID
	request.WorkID = w.WorkID
	request.TaskType = MapTaskType

	reply := TaskReply{}
	call("Master.TaskFinish", request, &reply)

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

// writeKVToFile
func (w *Work) writeKVToFile(fileID int, nReduce int, kv []KeyValue) error {
	// 将kv写入到mr-workID-nReduce的文件中
	doubleKV := make([][]KeyValue, nReduce)

	for i, _ := range doubleKV {
		doubleKV[i] = make([]KeyValue, 0)
	}

	for _, v := range kv {
		index := w.keyReduceIndex(v.Key, nReduce)
		doubleKV[index] = append(doubleKV[index], v)
	}

	for i := 0; i < nReduce; i++ {
		tempFile, err := os.CreateTemp(".", "mr_temp")

		if err != nil {
			log.Printf("os.CreateTemp error %v", err)
			return err
		}

		enc := json.NewEncoder(tempFile)

		for _, v := range doubleKV[i] {
			err := enc.Encode(v)
			if err != nil {
				log.Printf("Decode error %v", err)
				return err
			}
		}

		outName := fmt.Sprintf("mr-%d-%d", fileID, i)

		tempFile.Close()
		// 重命名
		err = os.Rename(tempFile.Name(), outName)
		if err != nil {
			log.Printf("os.Rename error,err = %v,index is %v", err, i)
			return err
		}
	}

	return nil
}

func (w *Work) keyReduceIndex(key string, nReduce int) int {
	return ihash(key) % nReduce
}

func (w *Work) executeReduceTask(reply *TaskReply, reductf func(string, []string) string) error {

	log.Printf("executeReduceTask,reduceIndex is %v", reply.ReduceIndex)

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

	tempFile, err := os.CreateTemp(".", "mrTemp")

	if err != nil {
		log.Printf("CreateTemp error %v", err)
		return err
	}

	w.reduceKVToTempFile(intermediate, reductf, tempFile)
	tempFile.Close()

	// 重命名文件
	outName := fmt.Sprintf("mr-out-%v", reply.ReduceIndex)
	err = os.Rename(tempFile.Name(), outName)
	if err != nil {
		log.Printf("Rename error %v", err)
		return err
	}
	// 上报任务完成
	w.notifyReduceTaskFinish(reply.ReduceIndex)
	return nil
}

// 上报reduce task完成
func (w *Work) notifyReduceTaskFinish(reduceIndex int) error {

	request := TaskFinishArgs{}
	request.ReduceIndex = reduceIndex
	request.WorkID = w.WorkID
	request.TaskType = ReduceTaskType

	reply := TaskReply{}
	call("Master.TaskFinish", request, &reply)

	if reply.Accept {
		log.Printf("task accept work id %v", w.WorkID)
		return nil
	}
	str := fmt.Sprintf("task accept error work id %v", w.WorkID)
	err := errors.New(str)
	log.Printf("%v", err)

	return err
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

// readIntermediateFile 读取map任务的产物
func (w *Work) readIntermediateFile(fileID int, reduceID int) ([]KeyValue, error) {

	fileName := fmt.Sprintf("mr-%d-%d", fileID, reduceID)
	file, err := os.Open(fileName)

	defer file.Close()

	if err != nil {
		log.Printf("Open file error,workID = %v,err = %v", w.WorkID, err)
		return nil, err
	}
	dec := json.NewDecoder(file)
	res := make([]KeyValue, 0)

	for {
		var t KeyValue
		err = dec.Decode(&t)
		if err != nil {
			//log.Printf("Decode error %v", err)
			// todo 这里应该返回错误 但是有时候err的类型是io.EOF 这里要找一下问题
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

	w := Work{}
	task := &TaskReply{}

	for !task.IsTaskAllFinish {
		task = GetTask()

		switch task.TaskType {
		case MapTaskType:
			err := w.executeMapTask(task, mapf)
			if err != nil {
				log.Printf("executeMapTask error %v", err)
				return
			}
		case ReduceTaskType:
			err := w.executeReduceTask(task, reducef)
			if err != nil {
				log.Printf("executeReduceTask error %v", err)
				return
			}
		}
	}

	log.Printf("no more task,exit,bye!!!")
}

// GetTask worker向master获取一个任务
func GetTask() *TaskReply {
	args := ExampleArgs{}
	reply := &TaskReply{}
	call("Master.AssignTask", args, reply)

	return reply
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
