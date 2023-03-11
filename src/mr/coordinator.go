package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	mapTasks       map[string]bool
	reduceTasks    map[int][]string
	nReduce        int
	mapTaskLock    sync.Mutex
	reduceTaskLock sync.Mutex
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mapTaskLock.Lock()
	defer c.mapTaskLock.Unlock()

	if len(c.mapTasks) != 0 {
		for filePath, taskAssigned := range c.mapTasks {
			if taskAssigned {
				continue
			}

			c.mapTasks[filePath] = true

			reply.InputFilePaths = []string{filePath}
			reply.TaskName = "map"
			reply.NReduceTasks = c.nReduce

			log.Printf("Assinging map task with input file: %s\n", filePath)

			break
		}
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
	for _, taskAssigned := range c.mapTasks {
		done = done && taskAssigned
	}

	return done
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		mapTasks:    map[string]bool{},
		reduceTasks: map[int][]string{},
		nReduce:     nReduce,
	}

	for _, file := range files {
		c.mapTasks[file] = false
	}

	c.server()
	return &c
}
