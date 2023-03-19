package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
)

type taskState int

const (
	unassigned taskState = iota
	in_progress
	completed
)

type Coordinator struct {
	mapTasks       map[string]taskState
	reduceTasks    []taskState
	nReduce        int
	mapTaskLock    sync.Mutex
	reduceTaskLock sync.Mutex
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	allMapTasksCompleted := c.assignMapTask(args, reply)
	if !allMapTasksCompleted {
		return nil
	}

	c.assignReduceTask(args, reply)

	return nil
}

func (c *Coordinator) assignMapTask(args *GetTaskArgs, reply *GetTaskReply) bool {
	c.mapTaskLock.Lock()
	defer c.mapTaskLock.Unlock()

	allMapTasksCompleted := true
	for filePath, taskState := range c.mapTasks {
		taskAssigned := false
		switch taskState {
		case completed:
			allMapTasksCompleted = allMapTasksCompleted && true
			continue
		case in_progress:
			allMapTasksCompleted = false
			continue
		default:
			c.mapTasks[filePath] = in_progress

			reply.InputFilePaths = filePath
			reply.TaskName = "map"
			reply.NReduceTasks = c.nReduce

			taskAssigned = true
			allMapTasksCompleted = false

			// TODO: Perhaps add a prefix to indicate whether the log message came from the Coordinator or a Worker
			log.Printf("Assinging map task with input file: %s\n", filePath)
		}

		if taskAssigned {
			break
		}
	}

	return allMapTasksCompleted
}

func (c *Coordinator) assignReduceTask(args *GetTaskArgs, reply *GetTaskReply) {
	c.reduceTaskLock.Lock()
	defer c.reduceTaskLock.Unlock()

	for i, taskState := range c.reduceTasks {
		if taskState != unassigned {
			continue
		}

		c.reduceTasks[i] = in_progress

		reply.TaskName = "reduce"
		reply.InputFilePaths = fmt.Sprintf("mr-intermediate-%d", i)

		log.Printf("Assigning reduce task for bucket: %d\n", i)
		break
	}
}

func (c *Coordinator) FinishTask(args *FinishTaskArgs, reply *FinishTaskReply) error {
	if args.TaskName == "map" {
		c.mapTaskLock.Lock()
		defer c.mapTaskLock.Unlock()

		c.mapTasks[args.TaskIdentifier] = completed
		log.Printf("Marked map task for %s as complete\n", args.TaskIdentifier)
	} else if args.TaskName == "reduce" {
		c.reduceTaskLock.Lock()
		defer c.reduceTaskLock.Unlock()

		index, err := strconv.Atoi(args.TaskIdentifier)
		if err != nil {
			log.Println("Error occurred while converting reduce task identifier to integer")
			log.Println(err)
			return err
		}

		c.reduceTasks[index] = completed
		log.Printf("Marked reduce task for bucket %d as complete\n", index)
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
	for _, taskState := range c.reduceTasks {
		done = done && (taskState == completed)
	}

	return done
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		mapTasks:    map[string]taskState{},
		reduceTasks: make([]taskState, nReduce),
		nReduce:     nReduce,
	}

	for _, file := range files {
		c.mapTasks[file] = unassigned
	}

	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = unassigned
	}

	log.Printf("Number of reduce tasks is: %d\n", nReduce)

	c.server()
	return &c
}
