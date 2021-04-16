package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "time"
import "io/ioutil"
import "encoding/json"
import "sort"
import "strconv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }


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

	// Your worker implementation here.
	for {
		
		reply := requestTask()
		if reply == nil || reply.Err != OK  {
			time.Sleep(time.Second)

		}else {
			log.Printf("%d starts to work on task %v\n", os.Getpid(), reply)
			tmpfile := ""
			result := false
			if reply.TaskType == MAP {
				result = execMap(reply.Urls[0], mapf, reply.TaskID, &tmpfile)

			}else {
				result = execReduce(reply.Urls, reducef, reply.TaskID, reply.NReduce, &tmpfile)
			}
			if result {
				log.Printf("%d completes task %v\n", os.Getpid(), reply)
				doneTask(reply.TaskType, reply.TaskID,  tmpfile)
				
			}else {
				log.Printf("%d fails on task %v\n", os.Getpid(), reply)
			}
		}
	}
}

func execMap(url string, mapf func(string, string ) []KeyValue, taskID int, tmpfile *string) bool{
	filename := url  
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
		return false
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)

		return false
	}
	file.Close()

	kva := mapf(filename, string(content))

	intermediateName := "mr-tmp-map-" +  strconv.Itoa(taskID)

	iFile, err := os.Create(intermediateName)
	if err != nil {
		log.Fatalf("can not create %v", intermediateName)
		log.Println(err )
		return false
	}

	enc := json.NewEncoder(iFile)

	for _, kv := range kva {
		err := enc.Encode(&kv)
		if err != nil {
			log.Fatalf("can not encode %v", kv)
			log.Println(err )

			return false
		}
	}
	(*tmpfile) = intermediateName
	return true
}

func execReduce(filenames []string, reducef func(string, []string) string, taskID int, nReduce int, tmpfile *string) bool {
	intermediate := []KeyValue{}
	for _, filename := range filenames {
		kva := readkvs(filename)
		intermediate = append(intermediate, kva...)
	}
	sort.Sort(ByKey(intermediate))
	log.Println("length of intermediate is ", len(intermediate))
	
	oname := "mr-out-" + strconv.Itoa(taskID) 
	ofile, err  := ioutil.TempFile("", "tmp")
	if err != nil {
		log.Fatalf("can not create %v", oname)
		log.Println(err )

		return false
	}

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
		if ihash(intermediate[i].Key) % nReduce == taskID {
			output := reducef(intermediate[i].Key, values)
			if intermediate[i].Key == "school" {
				log.Println("school len is ", len(values))
			}
			// this is the correct format for each line of Reduce output.
			fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)	
		}

		i = j
	}
	os.Rename(ofile.Name(), oname)
	(*tmpfile) = oname

	return true
}

func readkvs(filename string) []KeyValue{
	kva := []KeyValue{}

	f, err:= os.Open(filename)
	if err != nil {
		log.Fatalf("can not open %v", filename)
		log.Println(err )

	}

	dec := json.NewDecoder(f)

	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		kva = append(kva, kv)
	}
	f.Close()
	return kva
}
func requestTask() *RequestTaskReply {
	log.Printf("worker %d requests a task \n", os.Getpid())
	args := RequestTaskArgs{}
	reply := RequestTaskReply {}

	args.WorkerID = os.Getpid()

	ret := call("Coordinator.RequestTask", &args, &reply)
	
	log.Printf("result is %t, reply is %v\n",ret, reply)

	if ret == false {
		return nil 
	}
	
	if reply.Err == OK {
		return &reply 
	}

	return nil 
}

func doneTask(taskType string, taskID int, fname string ) bool {
	log.Printf("worker %d done a task, %v, %d, %v\n", os.Getpid(), taskType,taskID, fname)
	args := DoneTaskArgs{}
	args.WorkerID = os.Getpid()
	args.TaskType = taskType
	args.TaskID = taskID
	args.TmpFileName = fname


	reply := DoneTaskReply {}

	ret := call("Coordinator.DoneTask", &args, &reply)

	log.Printf("resulttt is %t, reply is %v\n",ret, reply)

	if ret == false {
		return false 
	}
	
	if reply.Err == OK {
		return true 
	}

	return false 	
}

//
// example function to show how to make an RPC call to the coordinator.
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
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
