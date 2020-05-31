package mr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
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

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		ch := make(chan string)
		var job *RequestJobReply

		go func() {
			job = GetJob()
			fmt.Println("job", job, os.Getpid())
			ch <- "success"
		}()

		select {
		case <-ch:
			{
				switch job.Phase {
				case Map:
					DoMap(job.FileName, job.I, job.NumReduce, mapf)
					SendJobStatus("m"+strconv.Itoa(job.I), job.Phase, job.I)
				case Reduce:
					DoReduce(job.FileName, job.I, job.NumMap, job.NumReduce, reducef)
					SendJobStatus("r"+strconv.Itoa(job.I), job.Phase, job.I)
					fmt.Println("reduce", job)
				}
			}
		case <-time.After(10 * time.Second):
			fmt.Println("master failed to respond")
			break
		}
	}
}

//
// example function to show how to make an RPC call to the master.
//
func GetJob() *RequestJobReply {
	args := RequestJobArgs{}
	reply := RequestJobReply{}

	// send the RPC request, wait for the reply.
	call("Master.GiveJob", &args, &reply)
	return &reply
}

func SendJobStatus(i string, s string, I int) {
	args := SendJobStatusArgs{I: i}
	reply := SendJobStatusReply{}
	call("Master.AckJob", &args, &reply)
}

func DoMap(file string, mapTaskNumber int, nReduce int, mapF func(string, string) []KeyValue) {

	fileMap := make(map[string][]KeyValue)
	content, _ := ioutil.ReadFile(file)

	for _, kv := range mapF(file, string(content)) {
		key := kv.Key
		r := ihash(key) % nReduce
		fileName := mFileName(mapTaskNumber, r)
		fileMap[fileName] = append(fileMap[fileName], kv)
	}

	for i := range fileMap {
		writeMaps(i, fileMap[i])
	}
}

func writeMaps(fileName string, kvs []KeyValue) {
	f := openWriter(fileName)
	enc := json.NewEncoder(f)
	for _, kv := range kvs {
		error := enc.Encode(&kv)
		if error != nil {
			log.Fatal(error)
		}
	}
	f.Close()
}

func DoReduce(file string, taskNumber int, nMaps int, nReduce int, reducef func(string, []string) string) {
	results := getReduceInput(taskNumber, nMaps)

	sort.Slice(results, func(i, j int) bool {
		return strings.Compare(results[i].Key, results[j].Key) < 0
	})

	fileName := rFileName(taskNumber)
	writeReduceFile(fileName, results, reducef)
}

func getReduceInput(taskNumber int, nMaps int) []KeyValue {
	var results []KeyValue

	for i := 0; i < nMaps; i++ {
		fileName := mFileName(i, taskNumber)

		r, err := os.Open(fileName)
		if err != nil {
			continue
		}
		defer r.Close()

		dec := json.NewDecoder(r)

		for {
			var keyValue KeyValue
			if err := dec.Decode(&keyValue); err == io.EOF {
				break
			} else if err != nil {
				log.Fatal(err)
			}
			results = append(results, keyValue)
		}
	}
	return results
}

func writeReduceFile(fileName string, results []KeyValue, reducef func(string, []string) string) {
	var i, j int = 0, 0
	f := openWriter(fileName)
	w := bufio.NewWriter(f)
	for {
		if i >= len(results) {
			break
		}

		key := results[i].Key
		var slice []string
		for j = i; j < len(results) && results[j].Key == key; j++ {
			slice = append(slice, results[j].Value)
		}

		val := reducef(key, slice)

		fmt.Fprintf(w, "%v %v\n", key, val)
		w.Flush()

		i = j
	}
}

func openWriter(file string) *os.File {
	os.Remove(file)
	f, err := os.Create(file)
	if err != nil {
		log.Fatal(err)
	}
	return f
}

func mFileName(mapNumber int, reduceNumber int) string {
	return "mr-" + strconv.Itoa(mapNumber) + "-" + strconv.Itoa(reduceNumber)

}

func rFileName(reduceNumber int) string {
	return "mr-out" + strconv.Itoa(reduceNumber)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("unix", "mr-socket")
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
