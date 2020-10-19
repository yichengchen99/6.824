package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	//CallExample()

	for {
		args := JobArgs{}
		reply := JobReply{}

		call("Master.HandleJobRequest", &args, &reply)

		if reply.JobType == "map" {
			processMapTask(&reply, mapf)
		} else if reply.JobType == "reduce" {
			processReduceTask(&reply, reducef)
		} else {

		}

		time.Sleep(time.Second)
	}
}

func processMapTask(reply *JobReply, mapf func(string, string) []KeyValue) {
	fmt.Println("Processing Map Task:", reply.Index, reply.FileNames[0])

	file, err := os.Open(reply.FileNames[0])
	if err != nil {
		log.Fatalf("cannot open %v", reply.FileNames[0])
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", reply.FileNames[0])
	}
	file.Close()

	kva := mapf(reply.FileNames[0], string(content))

	reduceFileNames := []string{}
	reduceFiles := []*os.File{}

	for i := 0; i < reply.NReduce; i++ {
		fileName := "mr-" + strconv.Itoa(reply.Index) + "-" + strconv.Itoa(i)
		reduceFileNames = append(reduceFileNames, fileName)
		reduceFile, _ := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
		reduceFiles = append(reduceFiles, reduceFile)
	}

	for _, kv := range kva {
		reduceIndex := ihash(kv.Key) % reply.NReduce

		reduceFile := reduceFiles[reduceIndex]

		enc := json.NewEncoder(reduceFile)
		enc.Encode(&kv)
	}

	for i := 0; i < reply.NReduce; i++ {
		reduceFiles[i].Close()
	}

	args := DoneMapArgs{
		ReduceFileNames: reduceFileNames,
		MapIndex:        reply.Index,
	}

	doneMapReply := DoneMapReply{}

	fmt.Println("Map Task Done:", reply.Index, reply.FileNames[0])

	call("Master.HandleDoneMapRequest", &args, &doneMapReply)
}

func processReduceTask(reply *JobReply, reducef func(string, []string) string) {
	fmt.Println("Processing Reduce Task:", reply.Index)

	kva := []KeyValue{}

	for _, fileName := range reply.FileNames {
		reduceFile, _ := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)

		dec := json.NewDecoder(reduceFile)

		for {
			var kv KeyValue

			if err := dec.Decode(&kv); err != nil {
				break
			}

			kva = append(kva, kv)
		}

		reduceFile.Close()
	}

	sort.Sort(ByKey(kva))

	outFileName := "mr-s2-" + strconv.Itoa(reply.Index)
	outFile, _ := os.Create(outFileName)

	for i := 0; i < len(kva); {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}

		values := []string{}

		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}

		output := reducef(kva[i].Key, values)
		fmt.Fprintf(outFile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	outFile.Close()

	args := DoneReduceArgs{ReduceIndex: reply.Index}

	doneMapReply := DoneMapReply{}

	fmt.Println("Reduce Task Done:", reply.Index)

	call("Master.HandleDoneReduceRequest", &args, &doneMapReply)
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
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
