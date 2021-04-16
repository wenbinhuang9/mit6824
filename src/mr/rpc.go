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
const (
	OK = "OK"
	NOTASK = "NOTASK"
)
const (
	MAP = "map"
	REDUCE = "reduce"
)
type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}


// Add your RPC definitions here.

type RequestTaskArgs struct {
    WorkerID int 
}

type RequestTaskReply struct {
    TaskType string  
    TaskID int 
    Urls []string 
	Err string
	NReduce int 
}

type DoneTaskArgs struct {
    TaskType string  
    TaskID int 
	WorkerID int 
	TmpFileName string 
}
type DoneTaskReply struct {
    Err string
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
