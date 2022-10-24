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
	"time"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	id := os.Getpid()
	// ask for task always
	for {
		//fmt.Printf("Worker process %v is asking for task\n", id)
		serverReply := AskReply{}
		serverReply.FileName, serverReply.WorkType, serverReply.ReduceNum, serverReply.MapWorkNum = CallForAsk(id)
		if serverReply.WorkType == "map" {
			//fmt.Printf("Work process %v is working on map task %v\n", id, serverReply.FileName)
			// worker get "map" task
			file, err := os.Open(serverReply.FileName)
			if err != nil {
				log.Fatalf("cannot open %v", serverReply.FileName)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", serverReply.FileName)
			}
			file.Close()
			kva := mapf(serverReply.FileName, string(content))
			hashKva := make(map[int][]KeyValue)
			ofile := make([]*os.File, serverReply.ReduceNum)
			for i := 0; i < len(kva); i++ {
				hashed := ihash(kva[i].Key) % serverReply.ReduceNum
				hashKva[hashed] = append(hashKva[hashed], kva[i])
			}
			for i := 0; i < serverReply.ReduceNum; i++ {
				ofile[i], _ = ioutil.TempFile("", "mr-tmp-*")
				enc := json.NewEncoder(ofile[i])
				for _, kv := range hashKva[i] {
					enc.Encode(&kv)
				}
			}
			for i := 0; i < serverReply.ReduceNum; i++ {
				outname := "mr-" + serverReply.MapWorkNum + "-" + strconv.Itoa(i)
				ofile[i].Close()
				os.Rename(ofile[i].Name(), outname)
			}
			//fmt.Printf("File %v map is done\n", serverReply.FileName)
		} else if serverReply.WorkType == "reduce" {
			// worker get "map" task
			//fmt.Printf("Work process %v is working on reduce task %v\n", id, serverReply.FileName)
			intermediate := []KeyValue{}
			//get fileList of reduce task
			files, _ := filepath.Glob("mr-*-" + serverReply.FileName)
			for _, file := range files {
				ofile, _ := os.Open(file)
				dec := json.NewDecoder(ofile)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv)
				}
			}

			sort.Sort(ByKey(intermediate))
			oname := "mr-out-" + serverReply.FileName
			ofile, _ := ioutil.TempFile("", "mr-tmp-*")
			//ofile, _ := os.Create(oname)
			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)
				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
				i = j
			}
			ofile.Close()
			os.Rename(ofile.Name(), oname)
			//fmt.Printf("File %v reduce is done\n", serverReply.FileName)
		} else if serverReply.WorkType == "Finished All" {
			// tasks are all finished
			// stop asking for tasks
			//fmt.Println("Task are all finished")
			break
		} else if serverReply.WorkType == "wait" {
			//fmt.Println("Waiting")
			time.Sleep(time.Second)
			continue
		} else {
			//fmt.Printf("error FileName:%v from server!\n", serverReply.FileName)
			//fmt.Printf("error WorkType:%v from server!\n", serverReply.WorkType)
			//fmt.Printf("error ReduceNum:%v from server!\n", serverReply.ReduceNum)
			return
		}

		// uncomment to send the Example RPC to the coordinator.
		CallForFinish(serverReply.FileName, serverReply.WorkType)
	}
}

// test for worker call coordinator
func CallForAsk(pid int) (fileName string, workType string, reduceNum int, mapWorkNum string) {
	args := AskArgs{pid}
	reply := AskReply{}
	ok := call("Coordinator.Ask", &args, &reply)
	if ok {
		return reply.FileName, reply.WorkType, reply.ReduceNum, reply.MapWorkNum
	} else {
		//fmt.Printf("call failed!\n")
		return "error", "error", 0, "error"
	}
}

// test for worker call coordinator
func CallForFinish(fileName string, workType string) {
	args := FinishArgs{}
	args.FileName = fileName
	args.WorkType = workType
	reply := FinishReply{}

	ok := call("Coordinator.Finish", &args, &reply)
	if ok {
		//fmt.Printf("Work is finished!\n")
	} else {
		//fmt.Printf("call failed!\n")
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
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	//fmt.Println(err)
	return false
}
