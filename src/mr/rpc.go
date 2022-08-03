package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type TaskFinishArgs struct {
	FileID int
	WorkID int
}

// TaskReply master和worker通信的reply
type TaskReply struct {
	FileName string
	FileID   int
	NReduce  int
	WorkID   int
	// 任务类型
	TaskType int
	mapF     func(string, string) []KeyValue
	ReduceF  func()
	Accept   bool
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
