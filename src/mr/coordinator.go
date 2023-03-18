package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type taskState int

const (
	unassigned taskState = iota
	in_progress
	completed
)

type Coordinator struct {
	mapTasks map[string]taskState
	// reduceTasks    map[int][]string
	nReduce     int
	mapTaskLock sync.Mutex
	// reduceTaskLock sync.Mutex
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mapTaskLock.Lock()
	defer c.mapTaskLock.Unlock()

	performingMapTask := false
	for filePath, taskState := range c.mapTasks {
		if taskState != unassigned {
			continue
		}

		c.mapTasks[filePath] = in_progress

		reply.InputFilePaths = []string{filePath}
		reply.TaskName = "map"
		reply.NReduceTasks = c.nReduce

		performingMapTask = true

		// TODO: Perhaps add a prefix to indicate whether the log message came from the Coordinator or a Worker
		log.Printf("Assinging map task with input file: %s\n", filePath)

		break
	}

	if performingMapTask {
		return nil
	}

	return nil
}

func (c *Coordinator) FinishTask(args *FinishTaskArgs, reply *FinishTaskReply) error {
	c.mapTaskLock.Lock()
	defer c.mapTaskLock.Unlock()

	if args.TaskName == "map" {
		c.mapTasks[args.TaskIdentifier] = completed
		log.Printf("Marked map task for %s as complete\n", args.TaskIdentifier)
	}

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
	done := true
	for _, taskState := range c.mapTasks {
		done = done && (taskState == completed)
	}

	return done
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		mapTasks: map[string]taskState{},
		// reduceTasks: []task{},
		nReduce: nReduce,
	}

	for _, file := range files {
		c.mapTasks[file] = unassigned
	}

	log.Printf("Number of reduce tasks is: %d\n", nReduce)

	c.server()
	return &c
}
