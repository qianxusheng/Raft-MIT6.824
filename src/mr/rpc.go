package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

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

// task request and coordinator's reply
type GetTaskArgs struct{}

type GetTaskReply struct {
	TaskStatus string
	TaskId     int
	FileName   string // for doMapTask to read file.txt
	NReduce    int
	NMap       int
}

// task progress report from worker
type TaskProgressArgs struct {
	TaskStatus string
	TaskId     int
}

type TaskProgressReply struct{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
