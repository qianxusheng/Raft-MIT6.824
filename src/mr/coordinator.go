package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	files   []string
	nReduce int
	nMap    int

	// 状态码：0=Idle(未分配), 1=InProgress(进行中), 2=Completed(已完成)
	mapStatus    []int
	reduceStatus []int

	mapStartTime    []time.Time
	reduceStartTime []time.Time

	mu sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// if idle map task
	allMapDone := true
	for i, status := range c.mapStatus {
		if status == 0 {
			reply.FileName = c.files[i]
			reply.TaskId = i
			reply.NReduce = c.nReduce // map task need to know how many buckets for reduce task needed
			reply.TaskStatus = "map"

			c.mapStatus[i] = 1
			c.mapStartTime[i] = time.Now()
			return nil
		}

		if status != 2 {
			allMapDone = false
		}
	}

	// all current task dispatched, wait
	if !allMapDone {
		reply.TaskStatus = "wait"
		return nil
	}

	// all map task done, we can do reduce task now
	allReduceDone := true
	for i, status := range c.reduceStatus {
		if status == 0 {
			reply.NMap = c.nMap
			reply.TaskStatus = "reduce"
			reply.TaskId = i

			c.reduceStartTime[i] = time.Now()
			c.reduceStatus[i] = 1
			return nil
		}

		if status != 2 {
			allReduceDone = false
		}
	}

	// all current task dispatched, wait
	if !allReduceDone {
		reply.TaskStatus = "wait"
		return nil
	}

	// all map and reduce work done, all worker exit
	reply.TaskStatus = "exit"
	return nil
}

func (c *Coordinator) TaskDone(args *TaskProgressArgs, reply *TaskProgressReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if args.TaskStatus == "map" {
		c.mapStatus[args.TaskId] = 2
	} else if args.TaskStatus == "reduce" {
		c.reduceStatus[args.TaskId] = 2
	}

	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {

	// Your code here.
	for _, status := range c.mapStatus {
		if status != 2 {
			return false
		}
	}

	for _, status := range c.reduceStatus {
		if status != 2 {
			return false
		}
	}

	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// initialization
	c.files = files
	c.nReduce = nReduce
	c.nMap = len(files)

	c.mapStatus = make([]int, c.nMap)
	c.reduceStatus = make([]int, c.nReduce)

	c.mapStartTime = make([]time.Time, c.nMap)
	c.reduceStartTime = make([]time.Time, c.nReduce)

	go c.healthCheck()
	c.server()
	return &c
}

func (c *Coordinator) healthCheck() {
	for {
		time.Sleep(time.Second)

		// 在循环中，我们要手动unlock
		// 因为defer是在return前运行的，但是这个for loop永不退出
		// 允许重复执行
		c.mu.Lock()
		for i, status := range c.mapStatus {
			if status == 1 && time.Since(c.mapStartTime[i]) > 10*time.Second {
				c.mapStatus[i] = 0
			}
		}

		for i, status := range c.reduceStatus {
			if status == 1 && time.Since(c.reduceStartTime[i]) > 10*time.Second {
				c.reduceStatus[i] = 0
			}
		}
		c.mu.Unlock()
	}
}
