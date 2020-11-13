package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strconv"
)

// KeyValue : Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type byKey []KeyValue

// for sorting by key.
func (a byKey) Len() int           { return len(a) }
func (a byKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// doMap applies map function to the given file, returns a kv array
func doMap(mapf func(string, string) []KeyValue, filename string) ([]KeyValue, error) {
	file, err := os.Open(filename)
	defer file.Close()
	if err != nil {
		return nil, fmt.Errorf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		return nil, fmt.Errorf("cannot read %v", filename)
	}
	kva := mapf(filename, string(content))
	return kva, nil
}

// Todo: how to close these files properly?
func getEncoders(fileNamePrefix string, cnt int) ([]*json.Encoder, error) {
	result := make([]*json.Encoder, 0, cnt)
	for i := 0; i < cnt; i++ {
		fileName := fileNamePrefix + strconv.Itoa(i)
		file, err := os.Create(fileName)
		if err != nil {
			return nil, fmt.Errorf("cannot open %v to save intermediate results", fileName)
		}
		enc := json.NewEncoder(file)
		result = append(result, enc)
	}
	return result, nil
}

func executeMapTask(mapf func(string, string) []KeyValue, taskInfo RequestForTaskReply) bool {
	log.Printf("[map] get task: '%v'", taskInfo)
	kva, err := doMap(mapf, taskInfo.FileToMap)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("[map] mapping function produces %v key-value pair(s)", len(kva))
	intermediateEncoders, err := getEncoders("mr-"+strconv.Itoa(taskInfo.TaskID)+"-", taskInfo.NReduce)
	if err != nil {
		log.Fatal(err)
	}
	for _, kv := range kva {
		bucket := ihash(kv.Key) % taskInfo.NReduce
		err := intermediateEncoders[bucket].Encode(kv)
		if err != nil {
			log.Fatal(err)
		}
	}
	log.Printf("[map] intermediate results have been written to files")
	args := UpdateTaskStatusArgs{
		IsMap:     true,
		TaskID:    taskInfo.TaskID,
		FileToMap: taskInfo.FileToMap,
		Success:   true,
	}
	log.Printf("[map] send request to update task '%v' status", taskInfo.FileToMap)
	reply := UpdateTaskStatusReply{}
	if !call("Master.UpdateTaskStatus", &args, &reply) {
		fmt.Println("[map] failed to contact the master, quitting...")
		return false
	}
	return true
}

func getKeyValuePairs(reduceTaskID int) []KeyValue {
	result := []KeyValue{}
	allFiles, err := filepath.Glob("mr-*-" + strconv.Itoa(reduceTaskID))
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("[reduce] find %v intermediate file(s)", len(allFiles))
	for _, f := range allFiles {
		file, err := os.Open(f)
		if err != nil {
			log.Fatal(err)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			result = append(result, kv)
		}
	}
	return result
}

func executeReduceTask(reducef func(string, []string) string, taskInfo RequestForTaskReply) bool {
	log.Printf("[reduce] get task: '%v'", taskInfo.TaskID)
	kvPairs := getKeyValuePairs(taskInfo.TaskID)
	log.Printf("[reduce] load %v key value pair(s) from files", len(kvPairs))
	log.Printf("[reduce] start to reduce")
	sort.Sort(byKey(kvPairs))
	oname := "mr-out-" + strconv.Itoa(taskInfo.TaskID)
	ofile, _ := os.Create(oname)
	i := 0
	for i < len(kvPairs) {
		j := i + 1
		for j < len(kvPairs) && kvPairs[j].Key == kvPairs[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kvPairs[k].Value)
		}
		output := reducef(kvPairs[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", kvPairs[i].Key, output)
		i = j
	}
	ofile.Close()
	log.Printf("[reduce] successfully write to %v", oname)
	args := UpdateTaskStatusArgs{
		IsMap:     false,
		TaskID:    taskInfo.TaskID,
		FileToMap: taskInfo.FileToMap,
		Success:   true,
	}
	log.Printf("[reduce] send request to update task '%v' status", taskInfo.TaskID)
	reply := UpdateTaskStatusReply{}
	if !call("Master.UpdateTaskStatus", &args, &reply) {
		fmt.Println("[reduce] failed to contact the master, quitting...")
		return false
	}
	return true
}

// Worker : main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	running := true
	for running {
		args := RequestForTaskArgs{}
		reply := RequestForTaskReply{}
		if !call("Master.RequestForTask", &args, &reply) {
			fmt.Println("failed to contact the master, quitting...")
			break
		}
		if reply.IsMap {
			running = executeMapTask(mapf, reply)
		} else {
			running = executeReduceTask(reducef, reply)
		}
	}
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
