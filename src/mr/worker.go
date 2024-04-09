package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

type worker struct {
	WorkerId      int
	LastHeartBeat time.Time
	Status        int //0-在线，1-忙碌，2-离线
	mapf          func(string, string) []KeyValue
	reducef       func(string, []string) string
}

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type RenameFile struct {
	OldName string
	NewName string
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
	// 每一个worker刚启动时，要去申请自己的编号id
	workId, err := RegisterWorkerRPC()
	// Your worker implementation here.
	if err != nil {
		log.Fatal("Register worker failed:", err)
	}
	for {
		task := AskTask(workId)
		switch task.TaskType {
		case MapTask:
			{
				files := DoMapTask(mapf, task)
				CallDone(workId, task.TaskId, files)
			}
		case ReduceTask:
			{

			}
		case WaittingTask:
			{
				time.Sleep(time.Second * 5)
			}
		case ExitTask:
			{
				time.Sleep(time.Second)
				fmt.Println("All tasks are Done ,will be exiting...")
				break
			}
		}
	}
	// uncomment to send the Example RPC to the coordinator.

}

func RegisterWorkerRPC() (int, error) {
	args := RegisterArg{}
	reply := RegisterReply{}
	ok := call("Coordinator.RegisterWorker", &args, &reply)
	if ok {
		fmt.Println(reply)
	} else {
		fmt.Println("call Failed")
	}
	return reply.WorkerId, nil
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func AskTask(workerId int) *TaskInfo {
	args := AskArg{workerId}
	reply := TaskInfo{}
	ok := call("Coordinator.GetTaskInfo", &args, &reply)
	if ok {
		fmt.Println(reply)
	} else {
		fmt.Println("call Failed")
	}
	return &reply

}
func CallDone(workerId int, TaskId int, files []RenameFile) {
	args := DoneArg{workerId, TaskId, files}
	reply := DoneReply{}
	ok := call("Coordinator.MarkFinished", &args, &reply)
	if ok {
		fmt.Println(reply)
	} else {
		fmt.Println("call Failed")
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

	fmt.Println(err)
	return false
}

func DoMapTask(mapf func(string, string) []KeyValue, task *TaskInfo) []RenameFile {
	fileName := task.FilePath
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", file)
	}
	// 通过io工具包获取content,作为mapf的参数
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", file)
	}
	file.Close()
	//进行mapf将文件中的k-v统计出来
	var intermediate []KeyValue
	intermediate = mapf(fileName, string(content))

	rn := task.NReducer
	// 分成reducer个数量的桶
	HashedKV := make([][]KeyValue, rn)
	result := make([]RenameFile, rn)
	for _, kv := range intermediate {
		i := ihash(kv.Key) % rn
		HashedKV[i] = append(HashedKV[i], kv)
	}

	for i := 0; i < rn; i++ {
		tempFile, err := ioutil.TempFile("", "mr-tmp-*")
		if err != nil {
			log.Fatalf("cannot create temp file for reducer %d", i)
		}
		enc := json.NewEncoder(tempFile)
		for _, kv := range HashedKV[i] {
			//这里会把kv结构体变成json格式然后写入文件oname中
			enc.Encode(kv)
		}
		oldName := tempFile.Name()
		tmp := RenameFile{
			OldName: oldName,
			NewName: "mr-" + strconv.Itoa(task.TaskId) + "-" + strconv.Itoa(i),
		}
		result = append(result, tmp)
		tempFile.Close()
	}
	return result
}
