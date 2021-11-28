package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	Tasks    []*Task
	NMap	 int
	NReduce  int
	AllFinished bool
	mu sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetTask(arg *ExampleArgs, reply *TaskInput) error {

	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.Tasks) == c.NMap {
		var inputFiles []string
		allFinished := true
		for _, task := range c.Tasks {
			if task.S != Finished {
				allFinished = false
				break
			}
			inputFiles = append(inputFiles, task.OutputPath)
		}
		if allFinished {
			// Create reduce tasks
			for i := 0; i < c.NReduce; i++ {
				reduceTask := Task{Id: i, InputPaths: inputFiles, S: Idle, IsReduce: true}
				c.Tasks = append(c.Tasks, &reduceTask)
			}
		}
	}

	// Fetch a task
	for _, task := range c.Tasks {
		// Attribute the task to another worker if timeouted
		timeout, _ := time.ParseDuration("10s")
		if task.S == InProgress && time.Now().Sub(task.StartTime) >= timeout {
			task.S = Idle
		}

		if task.S == Idle {
			reply.Id = task.Id
			if !task.IsReduce {
				reply.NReduce = c.NReduce
			}
			reply.Paths = task.InputPaths

			task.S = InProgress
			task.StartTime = time.Now()
			return nil
		}
	}

	if !c.AllFinished {	// Workers should not exit, since some of the tasks haven't finished yet
		return nil
	}

	return errors.New("no more task is available")
}

func (c *Coordinator) FinishTask(output *TaskOutput, reply *ExampleReply) error {

	c.mu.Lock()
	defer c.mu.Unlock()

	for _, task := range c.Tasks {
		if task.Id == output.Id && task.S == InProgress {
			task.S = Finished
			task.OutputPath = output.Directory
			return nil
		}
	}

	return errors.New("no task is in progress")
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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

	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.Tasks) == c.NMap + c.NReduce {
		for _, task := range c.Tasks {
			if task.S != Finished {
				return false
			}
		}
		c.AllFinished = true
	}

	return c.AllFinished
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Create map tasks with input files
	var mapTasks []*Task
	for i, file := range files {
		mapTasks = append(mapTasks, &Task{Id: i, InputPaths: []string{file}, S: Idle})
	}
	c := Coordinator{
		Tasks: mapTasks,
		NMap: len(files),
		NReduce: nReduce,
	}

	c.server()
	return &c
}