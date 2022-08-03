package mr

const (
	MapTaskType    = 1
	ReduceTaskType = 2
)

// TaskInf 定义一个任务类
type TaskInf interface {
}

// TaskInfo 任务信息
type TaskInfo struct {
	TaskType int // Task的类型 1 mapTask 2 ReduceTask
	WorkID   int
}

// TaskQueue 存放任务的队列
var TaskQueue = make(chan TaskInf)
