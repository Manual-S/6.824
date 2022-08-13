package mr

import (
	"log"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Master struct {
	Files   []string // 文件名集合
	nReduce int      // 有多少个reduce任务

	// 未分配的任务队列
	UndistributedMapTasks    chan int
	UndistributedReduceTasks chan int

	// 正在运行的任务队列
	RunningMapTask    chan TaskInfo
	RunningReduceTask chan TaskInfo

	// 已分配的任务
	AllocatedMapTask    ThreadSafetyMap
	AllocatedReduceTask ThreadSafetyMap

	IsMapTaskFinish    bool // 标记map任务是否完成
	IsReduceTaskFinish bool // 标记reduce任务是否完成
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// Done
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	if m.IsMapTaskFinish && m.IsReduceTaskFinish {
		ret = true
	}

	return ret
}

// AssignTask 分配任务
func (m *Master) AssignTask(request ExampleArgs, reply *TaskReply) error {

	if m.IsMapTaskFinish {
		// 分配reduce任务
		err := m.assignReduceTask(request, reply)
		if err != nil {
			log.Printf("assignReduceTask error %v", err)
			return err
		}
		return nil
	}

	// 分配Map任务
	err := m.assignMapTask(request, reply)
	if err != nil {
		log.Printf("assignMapTask error %v", err)
		return err
	}

	return nil

}

// TaskFinish 任务完成
func (m *Master) TaskFinish(request TaskFinishArgs, reply *TaskReply) error {

	if request.TaskType == MapTaskType {
		m.mapTaskFinish(request, reply)
	}

	return nil
}

func (m *Master) mapTaskFinish(request TaskFinishArgs, reply *TaskReply) {
	// 从map中移除分配的任务id
	m.AllocatedMapTask.Delete(request.FileID)
	reply.Accept = true
}

func (m *Master) reduceTaskFinish() {

}

// assignMapTask 分发map任务
func (m *Master) assignMapTask(request ExampleArgs, reply *TaskReply) error {

	log.Printf("start assignMapTask")

	if len(m.UndistributedMapTasks) == 0 {
		// 所有的map任务都被分配了
		log.Printf("all map distribute,len(UndistributedMapTasks) is 0")
		return nil
	}

	mapTaskID := <-m.UndistributedMapTasks

	m.AllocatedMapTask.Set(mapTaskID, true)

	reply.FileName = m.Files[mapTaskID]
	reply.TaskType = MapTaskType
	reply.NReduce = m.nReduce
	reply.FileID = mapTaskID

	log.Printf("assignMapTask suc,fileID is %v", mapTaskID)

	return nil
}

// assignReduceTask 分发reduce任务
func (m *Master) assignReduceTask(request ExampleArgs, reply *TaskReply) error {
	return nil
}

// MakeMaster
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {

	m := Master{}

	m.Files = files
	m.nReduce = nReduce
	m.IsReduceTaskFinish = false
	m.IsMapTaskFinish = false
	m.Files = files
	m.UndistributedMapTasks = make(chan int, len(files))
	m.AllocatedMapTask = NewThreadSafetyMap()
	for l, _ := range files {
		m.UndistributedMapTasks <- l
	}

	m.server()
	return &m
}
