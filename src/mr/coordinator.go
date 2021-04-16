package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"



const (
    IDLE = 0
    INPROCESS = 1
    COMPLETED = 2
)

type task struct {
    taskID int 
    tasktype string 
    status  int 
    urls []string 
	wokerID int
	startTime int64 	
}
type Err string 

type Coordinator struct {
	// Your definitions here.
	mu sync.Mutex
	maptasks []task
	mapDone int 
	reducetasks []task 
	reduceDone int 
	tmpfiles []string 
	nReduce int 
}

func makeTimestamp() int64 {
    return time.Now().UnixNano() / int64(time.Millisecond)
}

func (c *Coordinator) detectCrashWorker() error {
	log.Println("start to detect crashed workers")

	for {
		time.Sleep(time.Second)

		c.mu.Lock()

		for i, _ := range c.maptasks {
			var curTime  = makeTimestamp()
			if c.maptasks[i].status == INPROCESS  && (curTime - c.maptasks[i].startTime) > 1000 * 30 {
				log.Printf("tasks executes more than 10 seconds, %v, curime %d, ", c.maptasks[i], curTime)
				c.maptasks[i].wokerID = -1
				c.maptasks[i].status = IDLE 
			}
		}

		for i,_ := range c.reducetasks {
			var curTime = makeTimestamp()
			if c.reducetasks[i].status == INPROCESS  && (curTime - c.reducetasks[i].startTime) > 1000 * 30 {
				log.Printf("tasks executes more than 10 seconds, %v, curime %d",c.reducetasks[i], curTime)
				c.reducetasks[i].wokerID = -1
				c.reducetasks[i].status = IDLE 
			}
		}
		c.mu.Unlock()
	}
	return nil 
}




// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) assignIdleTask(wokerID int ) *task {
	c.mu.Lock()
	defer c.mu.Unlock()

	for i, _ := range c.maptasks {
		if c.maptasks[i].status == IDLE {
			c.maptasks[i].status = INPROCESS
			c.maptasks[i].wokerID = wokerID
			c.maptasks[i].startTime = makeTimestamp()
			return &c.maptasks[i] 
		}
	}
	for i, _:= range c.reducetasks {
		if c.reducetasks[i].status == IDLE {
			c.reducetasks[i].status = INPROCESS
			c.reducetasks[i].wokerID = wokerID
			c.reducetasks[i].startTime = makeTimestamp()
			return &c.reducetasks[i] 
		}
	}

	return nil 
}
func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	log.Printf("start Request Task %v\n", args)
	t := c.assignIdleTask(args.WorkerID)
	if t != nil {
		log.Printf("task %v assigned to worker %d \n", *t, args.WorkerID)
		reply.TaskType = t.tasktype
		reply.TaskID = t.taskID
		if reply.TaskType == MAP {
			reply.Urls = []string {t.urls[0]}
		}else {
			reply.Urls = c.tmpfiles
		}

		reply.NReduce = c.nReduce
		reply.Err = OK
	} else {
		reply.Err = NOTASK 
	}

	return nil 
}

func (c *Coordinator) DoneTask(args *DoneTaskArgs, reply *DoneTaskReply) error  {
	log.Printf("start DoneTask %v\n", args)
	c.mu.Lock()
	defer c.mu.Unlock()

	var t task
	ti := args.TaskID
	if (args.TaskType == MAP) {
		t = c.maptasks[ti]

		if( args.WorkerID != t.wokerID) {
			log.Printf("unmatched worker id task worker id %d, args workerID = %d\n", t.wokerID, args.WorkerID)
			// the task has been assigned to another worker
			// just ignore it 
			reply.Err = OK

			return nil 
		}
		c.mapDone += 1
		c.tmpfiles = append(c.tmpfiles, args.TmpFileName)
		c.maptasks[ti].status = COMPLETED


	}else {
		// reduce 
		t = c.reducetasks[ti]

		if( args.WorkerID != t.wokerID) {
			log.Printf("unmatched worker id task worker id %d, args workerID = %d\n", t.wokerID, args.WorkerID)
			reply.Err = OK
			return nil 
		}

		c.reduceDone += 1
		c.reducetasks[ti].status = COMPLETED
	}

	if c.mapDone == len(c.maptasks) && len(c.reducetasks) == 0  {
		log.Println("map tasks all done")
		// map finishes , start reduce work 
		for i := 0; i < c.nReduce; i++ {
			t := task{i, REDUCE, IDLE, c.tmpfiles, -1, -1}
			c.reducetasks = append(c.reducetasks, t)
		}
	}

	reply.Err = OK

	return nil 
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	if (c.mapDone == len(c.maptasks) && c.reduceDone == len(c.reducetasks)) {
		ret = true 
	}

	return ret
}

func initC(c *Coordinator, files []string, nReduce int ) {
	// init map task

	c.nReduce = nReduce

	for i, file := range files {
		t := task {i, MAP, IDLE, []string{file}, -1, -1}
		c.maptasks = append(c.maptasks ,t )
	}
	// todo what else to new ? 
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}


	// Your code here.

	initC(&c, files, nReduce)

	log.Println("Coordinator starts server")
	c.server()

	f, err := os.OpenFile("testlogfile", os.O_RDWR | os.O_CREATE | os.O_APPEND, 0666)
	if err != nil {
	    log.Fatalf("error opening file: %v", err)
	}
	defer f.Close()

	log.SetOutput(f)
	go c.detectCrashWorker()
	return &c
}
