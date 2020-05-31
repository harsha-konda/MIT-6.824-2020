package mr

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
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
	mapTaskStatus    map[string]bool
	reduceTaskStatus map[string]bool
	mu               sync.Mutex
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
	time.Sleep(5 * time.Second)
}

func (m *Master) performReduce() {
	for i := 0; i < m.nReduceTasks; i++ {
		m.fileIndexChan <- i
	}
	m.ret = true
}

func (m *Master) AckJob(sendJobStatusArgs *SendJobStatusArgs, sendJobStatusReply *SendJobStatusReply) error {
	i := sendJobStatusArgs.I
	if m.phase != Reduce {
		m.mu.Lock()
		m.mapTaskStatus[i] = true
		m.mu.Unlock()
	} else {
		m.mu.Lock()
		m.reduceTaskStatus[i] = true
		m.mu.Unlock()
	}
	return nil
}

func (m *Master) GiveJob(requestJobArgs *RequestJobArgs, requestJobReply *RequestJobReply) error {
	switch m.phase {
	case Map:
		m.GiveMapJob(requestJobArgs, requestJobReply)
	case Reduce:
		m.GiveReduceJob(requestJobArgs, requestJobReply)
	case WaitForMap:
		time.Sleep(1 * time.Second)
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

		go m.trackTimeOut(index, m.phase)
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

		go m.trackTimeOut(index, m.phase)

	} else {
		log.Fatal("something fucked up")
	}
}
func (m *Master) getValue(job int, phase string) bool {

	if phase != Reduce {
		return m.mapTaskStatus["m"+strconv.Itoa(job)]
	} else {
		return m.reduceTaskStatus["r"+strconv.Itoa(job)]
	}
}
func (m *Master) trackTimeOut(job int, phase string) {
	c := make(chan bool, 1)
	go func() {
		for {
			val := m.getValue(job, phase)
			if val {
				break
			}
			time.Sleep(1 * time.Second)
		}
		c <- true
	}()

	select {
	case <-c:
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
		for _, v := range m.mapTaskStatus {
			outcome = outcome && v
		}

		if outcome {
			m.phase = Reduce
		} else {
			time.Sleep(2 * time.Second)
			continue
		}

		for _, v := range m.reduceTaskStatus {
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
		files:            files,
		nMapTasks:        nMapTasks,
		nReduceTasks:     nReduce,
		phase:            Map,
		fileIndexChan:    make(chan int, nReduce),
		mapTaskStatus:    make(map[string]bool),
		reduceTaskStatus: make(map[string]bool),
	}
	for i := 0; i < nMapTasks; i++ {
		m.mapTaskStatus["m"+strconv.Itoa(i)] = false
	}

	for i := 0; i < nReduce; i++ {
		m.reduceTaskStatus["r"+strconv.Itoa(i)] = false
	}

	go m.schedule()
	go m.trackSuccess()
	m.server()

	return &m
}
