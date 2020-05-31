package mr

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)
import "net"
import "net/rpc"
import "net/http"

type Master struct {
	// Your definitions here.
	files        []string
	nMapTasks    int
	nReduceTasks int
	phase        string
	success      chan bool
	ret          bool

	//sync variable
	fileIndexChan chan int
	//taskTracker map[int]chan string
	taskStatus map[string]bool
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) schedule() {
	for {
		switch m.ret {
		case true:
			break
		default:
			switch m.phase {
			case Map:
				m.performMap()
			case Reduce:
				m.performReduce()
			}

		}
	}
}

func (m *Master) performMap() {
	for i := 0; i < len(m.files); i++ {
		m.fileIndexChan <- i
	}
	m.phase = Reduce
}

func (m *Master) performReduce() {
	for i := 0; i < m.nReduceTasks; i++ {
		m.fileIndexChan <- i
	}
	m.ret = true
}

func (m *Master) AckJob(sendJobStatusArgs *SendJobStatusArgs, sendJobStatusReply *SendJobStatusReply) error {
	i := sendJobStatusArgs.I
	m.taskStatus[i] = true
	return nil
}

func (m *Master) GiveJob(requestJobArgs *RequestJobArgs, requestJobReply *RequestJobReply) error {
	switch m.phase {
	case Map:
		m.GiveMapJob(requestJobArgs, requestJobReply)
	case Reduce:
		m.GiveReduceJob(requestJobArgs, requestJobReply)
	}
	return nil
}

func (m *Master) GiveMapJob(requestJobArgs *RequestJobArgs, requestJobReply *RequestJobReply) {
	index, ok := <-m.fileIndexChan
	if ok {
		file := m.files[index]

		requestJobReply.Phase = m.phase
		requestJobReply.FileName = file
		requestJobReply.Done = false
		requestJobReply.NumReduce = m.nReduceTasks
		requestJobReply.NumMap = m.nMapTasks

		requestJobReply.I = index

		go m.trackTimeOut(index)
	} else {
		requestJobReply.Done = true
	}
}

func (m *Master) GiveReduceJob(requestJobArgs *RequestJobArgs, requestJobReply *RequestJobReply) {
	index, ok := <-m.fileIndexChan
	if ok {
		file := rFileName(index)
		requestJobReply.Phase = m.phase
		requestJobReply.FileName = file
		requestJobReply.Done = false
		requestJobReply.NumReduce = m.nReduceTasks
		requestJobReply.NumMap = m.nMapTasks
		requestJobReply.I = index

		//go m.trackTimeOut(file)
	} else {
		requestJobReply.Done = true
	}
}

func (m *Master) trackTimeOut(job int) {
	c := make(chan bool, 1)
	go func() {
		//<- m.taskTracker[job]
		c <- true
	}()

	select {
	case <-c:
		fmt.Println("succcess %d", job)
	case <-time.After(10 * time.Second):
		fmt.Println("task timeout")
		m.fileIndexChan <- job
	}
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()

	os.Remove("mr-socket")
	l, e := net.Listen("unix", "mr-socket")

	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//

func (m *Master) trackSuccess() {
	for {
		var outcome = true
		for _, v := range m.taskStatus {
			outcome = outcome && v
		}

		if outcome {
			m.ret = true
			return
		}

		time.Sleep(2 * time.Second)
	}
}

func (m *Master) Done() bool {
	ret := m.ret
	return ret
}

//
// create a Master.
//
func MakeMaster(files []string, nReduce int) *Master {
	nMapTasks := len(files)
	m := Master{
		files:         files,
		nMapTasks:     nMapTasks,
		nReduceTasks:  nReduce,
		phase:         Map,
		fileIndexChan: make(chan int),
		taskStatus:    make(map[string]bool),
	}
	for i := 0; i < nMapTasks; i++ {
		m.taskStatus["m"+strconv.Itoa(i)] = false
	}

	for i := 0; i < nReduce; i++ {
		m.taskStatus["r"+strconv.Itoa(i)] = false
	}

	go m.schedule()
	go m.trackSuccess()
	m.server()

	return &m
}
