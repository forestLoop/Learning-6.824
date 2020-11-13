package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type taskStatus bool

const (
	processing taskStatus = true
	toDo       taskStatus = false
)

type taskInfo struct {
	Status     taskStatus
	ID         int
	LastUpdate time.Time
}

// Master is the master node for our map/reduce system
type Master struct {
	nReduce       int
	tasksToMap    map[string]*taskInfo
	tasksToReduce map[int]*taskInfo
	tasksMutex    sync.Mutex
}

// RequestForTask returns one task to the calling worker;
// if there's no remaining task, returns an error to let workers quit themselves
func (m *Master) RequestForTask(args *RequestForTaskArgs, reply *RequestForTaskReply) error {
	for {
		m.tasksMutex.Lock()
		if len(m.tasksToMap) != 0 { // map stage
			for t, info := range m.tasksToMap {
				if info.Status == toDo {
					log.Printf("[map] issue a ToDo task: '%v'", t)
				} else if info.LastUpdate.Add(10 * time.Second).Before(time.Now()) {
					log.Printf("[map] task '%v' seems to get stuck, re-issue it", t)
				} else {
					continue
				}
				reply.IsMap = true
				reply.FileToMap = t
				reply.NReduce = m.nReduce
				reply.TaskID = info.ID
				info.Status = processing
				info.LastUpdate = time.Now()
				m.tasksMutex.Unlock()
				return nil
			}
			log.Print("[map] waiting for unfinished task(s)")
		} else if len(m.tasksToReduce) != 0 { // reduce stage
			for _, info := range m.tasksToReduce {
				if info.Status == toDo {
					log.Printf("[reduce] issue a ToDo task: '%v'", info.ID)
				} else if info.LastUpdate.Add(10 * time.Second).Before(time.Now()) {
					log.Printf("[reduce] task '%v' seems to get stuck, re-issue it", info.ID)
				} else {
					continue
				}
				reply.IsMap = false
				reply.TaskID = info.ID
				m.tasksMutex.Unlock()
				return nil
			}
			log.Print("[reduce] waiting for unfinished task(s)")
		} else { // done!
			m.tasksMutex.Unlock()
			return fmt.Errorf("all map/reduce tasks have been finished")
		}
		m.tasksMutex.Unlock()
		time.Sleep(time.Second)
	}
}

// UpdateTaskStatus updates the status of map/reduce tasks
func (m *Master) UpdateTaskStatus(args *UpdateTaskStatusArgs, reply *UpdateTaskStatusReply) error {
	m.tasksMutex.Lock()
	defer m.tasksMutex.Unlock()
	if args.IsMap {
		if _, ok := m.tasksToMap[args.FileToMap]; !ok {
			log.Printf("[map] invalid task to update: %v", args.FileToMap)
			reply.Success = false
			return nil
		}
		if args.Success {
			log.Printf("[map] update task '%v' as finished", args.FileToMap)
			delete(m.tasksToMap, args.FileToMap)
		} else {
			log.Printf("[map] update task '%v' as ToDo", args.FileToMap)
			m.tasksToMap[args.FileToMap].Status = toDo
			m.tasksToMap[args.FileToMap].LastUpdate = time.Now()
		}
	} else {
		if _, ok := m.tasksToReduce[args.TaskID]; !ok {
			log.Printf("[reduce] invalid task to update: %v", args.TaskID)
			reply.Success = false
			return nil
		}
		if args.Success {
			log.Printf("[reduce] update task '%v' as finished", args.TaskID)
			delete(m.tasksToReduce, args.TaskID)
		} else {
			log.Printf("[reduce] update task '%v' as ToDo", args.TaskID)
			m.tasksToReduce[args.TaskID].Status = toDo
			m.tasksToReduce[args.TaskID].LastUpdate = time.Now()
		}
	}
	reply.Success = true
	return nil
}

// server : start a thread that listens for RPCs from worker.go
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// Done : main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {
	m.tasksMutex.Lock()
	defer m.tasksMutex.Unlock()
	return len(m.tasksToMap) == 0 && len(m.tasksToReduce) == 0
}

// MakeMaster : create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	maps := make(map[string]*taskInfo)
	for i, f := range files {
		maps[f] = &taskInfo{toDo, i, time.Now()}
	}
	reduces := make(map[int]*taskInfo)
	for i := 0; i < nReduce; i++ {
		reduces[i] = &taskInfo{toDo, i, time.Now()}
	}
	m := Master{
		nReduce:       nReduce,
		tasksToMap:    maps,
		tasksToReduce: reduces,
	}
	m.server()
	return &m
}
