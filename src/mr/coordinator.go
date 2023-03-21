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
	"time"
)

type taskState int

const (
	unassigned taskState = iota
	in_progress
	completed
)

const timeoutDuration = 10 * time.Second

type Task struct {
	state     taskState
	timeoutAt time.Time
}

type Coordinator struct {
	mapTasks       map[string]*Task
	reduceTasks    []*Task
	nReduce        int
	mapTaskLock    sync.Mutex
	reduceTaskLock sync.Mutex
	ticker         *time.Ticker
	doneChan       chan bool
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
	for filePath, task := range c.mapTasks {
		taskAssigned := false
		switch task.state {
		case completed:
			allMapTasksCompleted = allMapTasksCompleted && true
			continue
		case in_progress:
			allMapTasksCompleted = false
			continue
		default:
			task.state = in_progress
			task.timeoutAt = time.Now().Add(timeoutDuration)

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

	for i, task := range c.reduceTasks {
		if task.state != unassigned {
			continue
		}

		task.state = in_progress
		task.timeoutAt = time.Now().Add(timeoutDuration)

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

		task := c.mapTasks[args.TaskIdentifier]
		task.state = completed

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

		task := c.reduceTasks[index]
		task.state = completed

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

func (c *Coordinator) startTimeoutRoutine() {
	go func() {
		for {
			select {
			case <-c.ticker.C:
				c.mapTaskLock.Lock()
				for filePath, task := range c.mapTasks {
					if task.state == in_progress && time.Now().After(task.timeoutAt) {
						task.state = unassigned

						log.Printf("Map task for %s timed out.\n", filePath)
					}
				}
				c.mapTaskLock.Unlock()

				c.reduceTaskLock.Lock()
				for i, task := range c.reduceTasks {
					if task.state == in_progress && time.Now().After(task.timeoutAt) {
						task.state = unassigned
						task.timeoutAt = time.Now().Add(timeoutDuration)

						log.Printf("Reduce task for bucket %d timed out.\n", i)
					}
				}
				c.reduceTaskLock.Unlock()
			case <-c.doneChan:
				return
			}
		}
	}()
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.reduceTaskLock.Lock()
	defer c.reduceTaskLock.Unlock()

	done := true
	for _, task := range c.reduceTasks {
		done = done && (task.state == completed)
	}

	return done
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		mapTasks:    map[string]*Task{},
		reduceTasks: make([]*Task, nReduce),
		nReduce:     nReduce,
		ticker:      time.NewTicker(timeoutDuration),
		doneChan:    make(chan bool),
	}

	for _, file := range files {
		c.mapTasks[file] = &Task{state: unassigned, timeoutAt: time.Now()}
	}

	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = &Task{state: unassigned, timeoutAt: time.Now()}
	}

	log.Printf("Number of reduce tasks is: %d\n", nReduce)

	c.server()
	c.startTimeoutRoutine()
	return &c
}
