package mr

import (
	"log"
	"time"
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

	// 已分配的任务
	AllocatedMapTask    ThreadSafetyMap
	AllocatedReduceTask ThreadSafetyMap

	IsMapTaskFinish    bool // 标记map任务是否完成
	IsReduceTaskFinish bool // 标记reduce任务是否完成

	MaxWorkTime float64 // 最大工作时间
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

	switch request.TaskType {
	case MapTaskType:
		m.mapTaskFinish(request, reply)
	case ReduceTaskType:
		m.reduceTaskFinish(request, reply)
	default:
		panic("other TaskType")
	}

	return nil
}

func (m *Master) mapTaskFinish(request TaskFinishArgs, reply *TaskReply) {
	// 从map中移除分配的任务id
	m.AllocatedMapTask.Delete(request.FileID)
	reply.Accept = true
}

func (m *Master) reduceTaskFinish(request TaskFinishArgs, reply *TaskReply) {
	m.AllocatedReduceTask.Delete(request.ReduceIndex)
	reply.Accept = true
}

// assignMapTask 分发map任务
func (m *Master) assignMapTask(request ExampleArgs, reply *TaskReply) error {

	log.Printf("start assignMapTask")

	if len(m.UndistributedMapTasks) == 0 && m.AllocatedMapTask.Size() == 0 {
		// 所有的map任务都被分配了
		// 且分配的任务都执行完了
		m.IsMapTaskFinish = true
		return nil
	}

	mapTaskID := <-m.UndistributedMapTasks

	task := Task{
		StartTime: time.Now(),
	}

	m.AllocatedMapTask.Set(mapTaskID, task)

	reply.FileName = m.Files[mapTaskID]
	reply.TaskType = MapTaskType
	reply.NReduce = m.nReduce
	reply.FileID = mapTaskID

	reply.IsTaskAllFinish = m.IsMapTaskFinish && m.IsReduceTaskFinish

	log.Printf("assignMapTask suc,fileID is %v", mapTaskID)

	return nil
}

// assignReduceTask 分发reduce任务
func (m *Master) assignReduceTask(request ExampleArgs, reply *TaskReply) error {

	log.Printf("start assignReduceTask")

	if len(m.UndistributedReduceTasks) == 0 && m.AllocatedReduceTask.Size() == 0 {
		// 所有的reduce任务都分配完了
		m.IsReduceTaskFinish = true
		reply.IsTaskAllFinish = m.IsMapTaskFinish && m.IsReduceTaskFinish
		return nil
	}

	reduceTaskID := <-m.UndistributedReduceTasks

	task := Task{
		StartTime: time.Now(),
	}

	m.AllocatedReduceTask.Set(reduceTaskID, task)

	reply.TaskType = ReduceTaskType
	reply.ReduceIndex = reduceTaskID
	reply.FileCount = len(m.Files)

	reply.IsTaskAllFinish = m.IsMapTaskFinish && m.IsReduceTaskFinish

	log.Printf("assignReduceTask success,ReduceIndex is %v", reduceTaskID)

	return nil
}

// loopAllWorkers 清除所有的超时任务
func (m *Master) loopAllWorkers() {

	for true {
		time.Sleep(5 * time.Second)
		m.loopAllMapWorkers()
		m.loopAllReduceWorkers()
	}

}

func (m *Master) loopAllMapWorkers() {
	m.AllocatedMapTask.RemoveTimeoutTask(m.UndistributedMapTasks, m.MaxWorkTime)
}

func (m *Master) loopAllReduceWorkers() {
	m.AllocatedReduceTask.RemoveTimeoutTask(m.UndistributedReduceTasks, m.MaxWorkTime)
}

func (t *ThreadSafetyMap) RemoveTimeoutTask(UndistributedMapTasks chan int, maxTime float64) {
	t.lock.Lock()
	defer t.lock.Unlock()

	now := time.Now()

	for k, v := range t.hash {
		if now.Sub(v.StartTime).Seconds() > maxTime {
			// 说明当前的work任务没有完成
			delete(t.hash, k)
			UndistributedMapTasks <- k
		}
	}
}

// MakeMaster
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {

	m := Master{}

	m.MaxWorkTime = 10 // 10秒
	m.Files = files
	m.nReduce = nReduce
	m.IsReduceTaskFinish = false
	m.IsMapTaskFinish = false
	m.Files = files
	m.UndistributedMapTasks = make(chan int, len(files))
	m.UndistributedReduceTasks = make(chan int, m.nReduce)
	m.AllocatedMapTask = NewThreadSafetyMap()
	m.AllocatedReduceTask = NewThreadSafetyMap()

	for i, _ := range files {
		m.UndistributedMapTasks <- i
	}

	for i := 0; i < m.nReduce; i++ {
		m.UndistributedReduceTasks <- i
	}
	go m.loopAllWorkers()
	m.server()
	return &m
}
