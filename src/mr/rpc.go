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

// RequestForTaskArgs is null currently
type RequestForTaskArgs struct {
}

// RequestForTaskReply contains information for a map/reduce task
type RequestForTaskReply struct {
	IsMap     bool
	TaskID    int
	NReduce   int
	FileToMap string
}

// UpdateTaskStatusArgs updates the status of a map/reduce task
type UpdateTaskStatusArgs struct {
	IsMap     bool
	TaskID    int
	FileToMap string
	Success   bool
}

// UpdateTaskStatusReply indicates whether the update is successful
type UpdateTaskStatusReply struct {
	Success bool
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
