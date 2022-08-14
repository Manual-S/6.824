package mr

import "time"

const (
	MapTaskType    = 1
	ReduceTaskType = 2
)

// Task 任务信息
type Task struct {
	TaskType    int // Task的类型 1 mapTask 2 ReduceTask
	WorkID      int
	FileID      int // 文件ID
	ReduceIndex int
	StartTime   time.Time // 任务开始的时间
}
