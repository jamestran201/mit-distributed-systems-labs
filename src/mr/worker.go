package mr

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

func findBucket(key string, nReduceTasks int) int {
	return ihash(key) % nReduceTasks
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(
	mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
) {

	for {
		time.Sleep(1 * time.Second)

		log.Println("Retrieving task")
		taskReply := CallGetTask()
		if taskReply == nil {
			log.Println("Received empty task. Skipping.")
			continue
		}

		switch taskReply.TaskName {
		case "map":
			log.Printf("Processing map task with input files: %s\n", taskReply.InputFilePaths)

			err := processMapTask(taskReply.InputFilePaths, taskReply.NReduceTasks, mapf)
			if err != nil {
				continue
			}
		case "reduce":
			log.Printf("Processing reduce task with input files: %s\n", taskReply.InputFilePaths)

			err := processReduceTask(taskReply.InputFilePaths, reducef)
			if err != nil {
				continue
			}
		}
	}
}

func processMapTask(inputFilePath string, nReduceTasks int, mapf func(string, string) []KeyValue) error {
	bytes, err := os.ReadFile(inputFilePath)
	if err != nil {
		log.Println("Error occurred while reading input file: ", inputFilePath)
		log.Println(err)
		return err
	}

	results := mapf(inputFilePath, string(bytes))
	bucketedResults := make([][]KeyValue, nReduceTasks)
	for _, kv := range results {
		bucket := findBucket(kv.Key, nReduceTasks)
		bucketedResults[bucket] = append(bucketedResults[bucket], kv)
	}

	for i := 0; i < len(bucketedResults); i++ {
		outputFile := fmt.Sprintf("mr-intermediate-%d-*", i)
		file, err := os.CreateTemp(".", outputFile)
		if err != nil {
			log.Println("An error occurred while creating an intermediate result file")
			log.Println(err)
			return err
		}

		defer file.Close()

		for j, kv := range bucketedResults[i] {
			if j > 0 {
				file.WriteString("\n")
			}

			_, err := file.WriteString(fmt.Sprintf("%v %v", kv.Key, kv.Value))
			if err != nil {
				log.Println("An error occurred while writing to an intermediate result file")
				log.Println(err)
				return err
			}
		}
	}

	CallFinishTask("map", inputFilePath)

	return nil
}

func processReduceTask(inputFilePrefix string, reducef func(string, []string) string) error {
	// Read intermediate result files

	pattern := fmt.Sprintf("%s*", inputFilePrefix)
	fileNames, err := filepath.Glob(pattern)
	if err != nil {
		log.Println("An error occurred while retrieving the list of intermediate files")
		log.Println(err)
		return err
	}

	valuesByKey := make(map[string][]string)

	for _, fileName := range fileNames {
		file, err := os.Open(fileName)
		if err != nil {
			log.Printf("Error opening intermediate file: %s\n", fileName)
			return err
		}

		defer file.Close()

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := scanner.Text()
			tokens := strings.Split(line, " ")
			key := tokens[0]
			value := tokens[1]

			if valuesByKey[key] == nil {
				valuesByKey[key] = make([]string, 0)
			}

			valuesByKey[key] = append(valuesByKey[key], value)
		}
	}

	// Write reduce results to output file

	results := map[string]string{}
	for key, values := range valuesByKey {
		results[key] = reducef(key, values)
	}

	bucket := strings.Split(inputFilePrefix, "-")[2]
	outputFileName := fmt.Sprintf("mr-out-%s", bucket)
	outFile, err := os.Create(outputFileName)
	if err != nil {
		log.Println("An error occurred while creating an output file")
		log.Println(err)
		return err
	}

	defer outFile.Close()

	for key, value := range results {
		_, err := outFile.WriteString(fmt.Sprintf("%v %v\n", key, value))
		if err != nil {
			log.Println("An error occurred while writing to an output file")
			log.Println(err)
			return err
		}
	}

	CallFinishTask("reduce", bucket)

	return nil
}

func CallGetTask() *GetTaskReply {
	reply := &GetTaskReply{}

	ok := call("Coordinator.GetTask", &GetTaskArgs{}, reply)
	if ok {
		return reply
	} else {
		log.Println("Error occurred while retrieving task from coordinator")
		return nil
	}
}

func CallFinishTask(taskName, taskIdentifier string) *FinishTaskReply {
	args := &FinishTaskArgs{
		TaskName:       taskName,
		TaskIdentifier: taskIdentifier,
	}
	reply := &FinishTaskReply{}

	ok := call("Coordinator.FinishTask", args, reply)
	if ok {
		return reply
	} else {
		log.Println("Error occurred while notifying the coordinator that the task is done")
		return nil
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		if strings.Contains(err.Error(), "connection refused") {
			log.Println("Coordinator is not running. Stopping worker.")
			os.Exit(0)
		}

		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	log.Println(err)
	return false
}
