package mr

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type mapTask struct {
	mapFile  string
	mapIndex int
	jobDone  bool
}

type reduceTask struct {
	reduceFiles []string
	reduceIndex int
	jobDone     bool
}

type Master struct {
	// Your definitions here.
	mapFiles     chan string
	mapFileIndex int
	mapTasks     map[int]*mapTask

	reduceIndexes chan int
	reduceTasks   map[int]*reduceTask

	nReduce int

	mu sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) HandleJobRequest(args *JobArgs, reply *JobReply) error {
	if len(m.mapFiles) > 0 {
		m.handleMapRequest(args, reply)
	} else if len(m.reduceIndexes) > 0 && len(m.mapTasks) == 0 {
		m.handleReduceRequest(args, reply)
	}
	return nil
}

func (m *Master) handleMapRequest(args *JobArgs, reply *JobReply) {
	reply.JobType = "map"

	fileName := <-m.mapFiles
	reply.FileNames = append(reply.FileNames, fileName)

	m.mu.Lock()

	reply.Index = m.mapFileIndex

	mapTask := mapTask{
		mapFile:  fileName,
		mapIndex: m.mapFileIndex,
		jobDone:  false,
	}
	m.mapTasks[m.mapFileIndex] = &mapTask

	m.mu.Unlock()

	reply.NReduce = m.nReduce

	fmt.Println("Distributing Map Task:", reply.Index, reply.FileNames[0])

	m.mapFileIndex++
	go m.checkMapTaskDone(reply.Index)
}

func (m *Master) checkMapTaskDone(mapIndex int) {
	time.Sleep(5 * time.Second)

	m.mu.Lock()

	if !m.mapTasks[mapIndex].jobDone {
		m.mapFiles <- m.mapTasks[mapIndex].mapFile
	} else {
		fmt.Println("Map Task Done:", mapIndex)
		delete(m.mapTasks, mapIndex)
	}

	m.mu.Unlock()
}

func (m *Master) handleReduceRequest(args *JobArgs, reply *JobReply) {
	reply.JobType = "reduce"

	reduceIndex := <-m.reduceIndexes

	reply.FileNames = m.reduceTasks[reduceIndex].reduceFiles

	reply.Index = reduceIndex

	reply.NReduce = m.nReduce

	fmt.Println("Distributing Reduce Task:", reply.Index)

	go m.checkReduceTaskDone(reply.Index)
}

func (m *Master) checkReduceTaskDone(reduceIndex int) {
	time.Sleep(10 * time.Second)

	if !m.reduceTasks[reduceIndex].jobDone {
		m.reduceIndexes <- reduceIndex
	} else {
		fmt.Println("Reduce Task Done:", reduceIndex)
		delete(m.reduceTasks, reduceIndex)
	}
}

func (m *Master) HandleDoneMapRequest(args *DoneMapArgs, reply *DoneMapReply) error {
	m.mu.Lock()
	m.mapTasks[args.MapIndex].jobDone = true
	m.mu.Unlock()

	for _, reduceFileName := range args.ReduceFileNames {
		slice := strings.FieldsFunc(reduceFileName, func(r rune) bool {
			if r == '-' {
				return true
			}

			return false
		})

		reduceIndex := slice[len(slice)-1]
		index, _ := strconv.Atoi(reduceIndex)

		m.mu.Lock()
		m.reduceTasks[index].reduceFiles = append(m.reduceTasks[index].reduceFiles, reduceFileName)
		m.mu.Unlock()
	}

	return nil
}

func (m *Master) HandleDoneReduceRequest(args *DoneReduceArgs, reply *DoneReduceReply) error {
	m.mu.Lock()
	m.reduceTasks[args.ReduceIndex].jobDone = true
	m.mu.Unlock()

	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrmaster.go calls Done() periodically to find s2
// if the entire job has finished.
//
func (m *Master) Done() bool {
	// Your code here.

	if len(m.mapFiles) == 0 && len(m.reduceIndexes) == 0 &&
		len(m.mapTasks) == 0 && len(m.reduceTasks) == 0 {
		fmt.Println("All Tasks Done, Exiting...")
		return true
	}

	return false
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.

	m.mapFiles = make(chan string, len(files))
	for _, file := range files {
		m.mapFiles <- file
	}
	m.mapTasks = make(map[int]*mapTask)

	m.reduceIndexes = make(chan int, nReduce)
	for i := 0; i < nReduce; i++ {
		m.reduceIndexes <- i
	}
	m.reduceTasks = make(map[int]*reduceTask)

	for i := 0; i < nReduce; i++ {
		m.reduceTasks[i] = &reduceTask{
			reduceFiles: []string{},
			reduceIndex: 0,
			jobDone:     false,
		}
	}

	m.nReduce = nReduce

	m.server()
	return &m
}
