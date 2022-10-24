package mr

import (
	//"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	lock               sync.Mutex
	state              string
	fileMap            map[string]string
	fileMapFinished    map[string]int
	fileReduceFinished map[string]int
	reduceNum          int
	toleranceSecond    int
}

// Your code here -- RPC handlers for the worker to call.
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) CheckFinish(task string) bool {
	c.lock.Lock()
	defer c.lock.Unlock()
	if task == "map" {
		for _, v := range c.fileMapFinished {
			if v != -2 {
				return false
			}
		}
	} else {
		for _, v := range c.fileReduceFinished {
			if v != -2 {
				return false
			}
		}
	}
	return true
}

// the RPC Ask
func (c *Coordinator) Ask(args *AskArgs, reply *AskReply) error {
	//fmt.Printf("Worker process %v is asking for task\n", args.Pid)
	reply.ReduceNum = c.reduceNum
	reply.MapWorkNum = ""
	
	c.lock.Lock()
	if c.state == "Map" {
		c.lock.Unlock()
		if !c.CheckFinish("map") {
			c.lock.Lock()
			for file, state := range c.fileMapFinished {
				if state == -1 {
					c.fileMapFinished[file] = args.Pid
					c.lock.Unlock()
					reply.FileName = file
					reply.WorkType = "map"
					reply.MapWorkNum = c.fileMap[file]
					//fmt.Printf("Map task %v is sent to process %v\n", file, args.Pid)
					return nil
				}
			}
			c.lock.Unlock()
			//fmt.Printf("process %v must wait\n", args.Pid)
			reply.FileName = ""
			reply.WorkType = "wait"
			return nil
		} else {
			//fmt.Println("Map tasks are all finished")
			c.lock.Lock()
			c.state = "Reduce"
			c.lock.Unlock()
			//fmt.Printf("\n")
			//fmt.Println("Reduce mode")
			//fmt.Printf("\n")
		}
	} else {
		c.lock.Unlock()
	}

	c.lock.Lock()
	if c.state == "Reduce" {
		c.lock.Unlock()
		if !c.CheckFinish("Reduce") {
			c.lock.Lock()
			for file, state := range c.fileReduceFinished {
				if state == -1 {
					c.fileReduceFinished[file] = args.Pid
					reply.FileName = file
					reply.WorkType = "reduce"
					c.lock.Unlock()
					//fmt.Printf("Reduce task %v is sent to process %v\n", file, args.Pid)
					return nil
				}
			}
			c.lock.Unlock()
			////fmt.Printf("process %v must wait\n", args.Pid)
			reply.FileName = ""
			reply.WorkType = "wait"
			return nil
		} else {
			c.lock.Lock()
			c.state = "Release workers"
			c.lock.Unlock()
			//fmt.Println("Reduce tasks are all finished")
			//fmt.Printf("\n")
			//fmt.Println("Release mode")
			//fmt.Printf("\n")
		}
	} else {
		c.lock.Unlock()
	}

	c.lock.Lock()
	if c.state == "Release workers" {
		c.lock.Unlock()
		reply.FileName = ""
		reply.WorkType = "Finished All"
		c.lock.Lock()
		c.state = "Finished All"
		c.lock.Unlock()
		return nil
	} else {
		c.lock.Unlock()
	}

	//fmt.Printf("error master state:%v\n", c.state)
	return nil
}

// the RPC Finish
func (c *Coordinator) Finish(args *FinishArgs, reply *FinishReply) error {
	if args.WorkType == "map" {
		//fmt.Printf("Map task %v is finished\n", args.FileName)
		c.lock.Lock()
		if c.fileMapFinished[args.FileName] == -1 {
			return nil
		} else {
			c.fileMapFinished[args.FileName] = -2
		}
		c.lock.Unlock()
	} else if args.WorkType == "reduce" {
		//fmt.Printf("Reduce task %v is finished\n", args.FileName)
		c.lock.Lock()
		if c.fileReduceFinished[args.FileName] == -1 {
			return nil
		} else {
			c.fileReduceFinished[args.FileName] = -2
		}
		c.lock.Unlock()
	} else {
		//fmt.Println("error worktype")
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
	c.lock.Lock()
	defer c.lock.Unlock()
	ret := true
	// Your code here.
	if c.state != "Finished All" {
		return !ret
	} else {
		// delete temp files in map task
		for i := 0; i < 10; i++ {
			filename := strconv.Itoa(i) + "*.tmp"
			files, _ := filepath.Glob(filename)
			for _, file := range files {
				os.Remove(file)
			}
		}
	}
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Your code here.
	c := Coordinator{
		state:              "Map",
		fileMap:            make(map[string]string, len(files)),
		fileMapFinished:    make(map[string]int, len(files)),
		fileReduceFinished: make(map[string]int, nReduce),
		reduceNum:          nReduce,
		toleranceSecond:    10,
	}
	i := 0
	for _, file := range files {
		c.fileMapFinished[file] = -1
		c.fileMap[file] = strconv.Itoa(i)
		i++
	}
	for i := 0; i < nReduce; i++ { 
		c.fileReduceFinished[strconv.Itoa(i)] = -1
	}
	c.server()
	/*
		fmt.Println("master start")
		fmt.Printf("\n")
		fmt.Println("Map mode")
		fmt.Printf("\n")
	*/
	go func() {
		for {
			time.Sleep(1)
			c.lock.Lock()
			if c.state == "Map" {
				c.lock.Unlock()
				for file, _ := range c.fileMap {
					c.lock.Lock()
					if c.fileMapFinished[file] >= 0 {
						c.lock.Unlock()
						select {
						case <-time.After(5 * time.Second):
							c.lock.Lock()
							if c.fileMapFinished[file] >= 0 {
								//fmt.Printf("map task %v recycle\n", c.fileMap[file])
								c.fileMapFinished[file] = -1
							}
							c.lock.Unlock()
						}
					} else {
						c.lock.Unlock()
					}
				}
			} else if c.state == "Reduce" {
				c.lock.Unlock()
				for i := 0; i < nReduce; i++ {
					c.lock.Lock()
					if c.fileReduceFinished[strconv.Itoa(i)] >= 0 {
						c.lock.Unlock()
						select {
						case <-time.After(5 * time.Second):
							c.lock.Lock()
							if c.fileReduceFinished[strconv.Itoa(i)] >= 0 {
								//fmt.Printf("reduce task %v recycle\n", strconv.Itoa(i))
								c.fileReduceFinished[strconv.Itoa(i)] = -1
							}
							c.lock.Unlock()
						}
					} else {
						c.lock.Unlock()
					}
				}
			} else {
				c.lock.Unlock()
				break
			}
		}
	} ()
	return &c
}
