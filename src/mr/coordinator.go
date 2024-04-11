package mr

import (
	"fmt"
	"io/fs"
	"io/ioutil"
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

type TaskInfo struct {
	TaskId         int
	TaskType       TaskType
	FilePath       []string
	NReducer       int
	AssignedWorker int
	FailedWorkers  []int
	TaskState      State
	StartTime      time.Time
}

type Coordinator struct {
	// Your definitions here.
	mapTasks     chan int
	reduceTasks  chan int
	taskMap      map[int]*TaskInfo //根据任务编号快速得到任务的详情，包括当前任务的状态以及分配给了哪一个worker
	mutex        sync.Mutex
	nReducer     int
	taskPhase    Phase
	files        []string
	nextWorkerId int
}

// 协调者看当前所处的阶段 map reduce alldone
type Phase int

const (
	MapPhase Phase = iota
	ReducePhase
	AllDone
)

// 任务的类型
type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	WaittingTask
	ExitTask
)

type State int

const (
	Idle State = iota
	Running
	Finished
)

// Your code here -- RPC handlers for the worker to call.
// 给worker分配任务
func (c *Coordinator) GetTaskInfo(args *AskArg, reply *TaskInfo) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	switch c.taskPhase {
	case MapPhase:
		{
			if len(c.mapTasks) > 0 {
				index := <-c.mapTasks
				taskInfo := c.taskMap[index]
				if taskInfo != nil {
					// 修改任务信息
					taskInfo.AssignedWorker = args.WorkerId
					taskInfo.TaskState = Running
					taskInfo.StartTime = time.Now()
					// 将任务信息指针赋值给reply
					*reply = *taskInfo
					go c.monitorTask(index, args.WorkerId)
				} else {
					reply.TaskType = WaittingTask
				}
			} else {
				// 还处于MapPhase阶段，但是mapTask都分发出去了，说明map的任务没有都完成,此时来请求的worker让他等待
				reply.TaskType = WaittingTask
				return nil
			}
		}
		break
	case ReducePhase:
		{
			if len(c.reduceTasks) > 0 {
				index := <-c.reduceTasks
				//修改任务信息
				c.taskMap[index].AssignedWorker = args.WorkerId
				c.taskMap[index].TaskState = Running
				c.taskMap[index].StartTime = time.Now()
				*reply = *c.taskMap[index]
			} else {
				reply.TaskType = WaittingTask
				return nil
			}
		}
		break
	default:
		reply.TaskType = ExitTask
	}
	return nil
}

func (c *Coordinator) monitorTask(index int, wid int) {
	select {
	case <-time.After(10 * time.Second):
		c.mutex.Lock()
		defer c.mutex.Unlock()
		task, exists := c.taskMap[index]
		state := task.TaskState
		assignedWorkerId := task.AssignedWorker
		taskType := task.TaskType
		if exists && state == Running && assignedWorkerId == wid {
			task.AssignedWorker = -1
			task.TaskState = Idle
			task.StartTime = time.Time{}
			task.FailedWorkers = append(task.FailedWorkers, wid)
			switch taskType {
			case MapTask:
				go func() { c.mapTasks <- index }()
			case ReduceTask:
				go func() { c.reduceTasks <- index }()
			default:
				log.Printf("Unknown task type %d for task %d\n", taskType, index)
			}
		}
	}

}

func (c *Coordinator) MarkFinished(args *DoneArg, reply *DoneReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	rwid := args.WorkerId
	taskId := args.TaskId
	task, exists := c.taskMap[taskId]
	if !exists {
		fmt.Printf("Task Id[%d] does not exist.\n", taskId)
		return nil
	}
	switch task.TaskType {
	case MapTask:
		if task.TaskState == Running {
			if rwid == task.AssignedWorker {
				task.TaskState = Finished
				fmt.Printf("Map task Id[%d] is finished.\n", task.TaskId)
				c.renameFiles(args.TempFiles)
			} else {
				fmt.Printf("Map task Id[%d] is now assigned by worker[%d],not worker[%d]", task.TaskId, task.AssignedWorker, rwid)
			}
		} else if task.TaskState == Idle {
			fmt.Printf("The worker[%d] is time out!\n", rwid)
		} else {
			fmt.Printf("Map task Id[%d] is finished,already ! ! !\n", task.TaskId)
		}
		if c.allMapTasksFinished() {
			c.initReduceTasks()
		}
		break
	case ReduceTask:
		if task.TaskState == Running {
			if rwid == task.AssignedWorker {
				task.TaskState = Finished
				fmt.Printf("Reduce task Id[%d] is finished.\n", task.TaskId)
				c.renameFiles(args.TempFiles)
			} else {
				fmt.Printf("Reduce task Id[%d] is now assigned by worker[%d], not worker[%d]\n", task.TaskId, task.AssignedWorker, rwid)
			}
		} else if task.TaskState == Idle {
			fmt.Printf("The worker[%d] reported after timeout!\n", rwid)
		} else {
			fmt.Printf("Reduce task Id[%d] is already finished!\n", task.TaskId)
		}

		if c.allReduceTasksFinished() {
			c.finalizeReduceTasks()
		}

	}

	return nil
}

// allMapTasksFinished 检查是否所有Map任务都已完成
func (c *Coordinator) allMapTasksFinished() bool {
	for _, task := range c.taskMap {
		if task.TaskType == MapTask && task.TaskState != Finished {
			return false
		}
	}
	return true
}

func (c *Coordinator) allReduceTasksFinished() bool {
	for _, task := range c.taskMap {
		if task.TaskType == ReduceTask && task.TaskState != Finished {
			return false
		}
	}
	return true
}

// initReduceTasks 初始化Reduce任务
func (c *Coordinator) initReduceTasks() {
	// 更新阶段为Reduce
	c.taskPhase = ReducePhase
	// 这里初始化Reduce任务，例如填充reduceTasks通道和设置任务状态
	c.makeReduceTasks()
}

func (c *Coordinator) finalizeReduceTasks() {
	c.taskPhase = AllDone

}

func (c *Coordinator) renameFiles(renameFiles []RenameFile) {
	for _, file := range renameFiles {
		err := os.Rename(file.OldName, file.NewName)
		if err != nil {
			log.Printf("Failed to rename file from %s to %s: %v\n", file.OldName, file.NewName, err)
			// 处理错误，例如记录日志或重试
		} else {
			log.Printf("Successfully renamed file from %s to %s\n", file.OldName, file.NewName)
		}
	}
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
//	reply.Y = args.X + 1
//	return nil
//}

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
// Done 主函数mr调用，如果所有task完成mr会通过此方法退出
func (c *Coordinator) Done() bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.taskPhase == AllDone {
		fmt.Printf("All tasks are finished,the coordinator will be exit! !")
		return true
	} else {
		return false
	}

}

// 为注册的worker提供一个唯一的id
func (c *Coordinator) RegisterWorker(args *RegisterArg, reply *RegisterReply) error {
	c.mutex.Lock()
	reply.WorkerId = c.nextWorkerId
	fmt.Printf("Worker registered successfully with ID: %d\n", c.nextWorkerId)
	c.nextWorkerId++
	c.mutex.Unlock()
	return nil
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
// 这个会被main/mrcoordinator.go 调用来创建一个协调者
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:        files,
		nReducer:     nReduce,
		mapTasks:     make(chan int, len(files)),
		reduceTasks:  make(chan int, nReduce),
		taskMap:      make(map[int]*TaskInfo, len(files)+nReduce), // map + reduce 任务元信息
		taskPhase:    MapPhase,
		nextWorkerId: 0,
	}
	c.makeMapTasks(files, nReduce)

	// Your code here.

	c.server()
	return &c
}

// 根据输入文件创建好MapTask
func (c *Coordinator) makeMapTasks(files []string, nReduce int) {

	for i, v := range files {
		id := i
		c.mapTasks <- id
		c.taskMap[id] = &TaskInfo{
			FilePath:  []string{v},
			TaskId:    id,
			TaskType:  MapTask,
			NReducer:  nReduce,
			TaskState: Idle,
		}
		fmt.Println("make a map task :", c.taskMap[id])
	}

}

func (c *Coordinator) makeReduceTasks() {
	reduceNum := c.nReducer
	baseId := len(c.files)
	// 一次性读取当前目录文件
	path, err := os.Getwd()
	if err != nil {
		log.Fatalf("Failed to get working directory: %v", err)
	}

	files, err := ioutil.ReadDir(path)
	if err != nil {
		log.Fatalf("Failed to read directory: %v", err)
	}
	for i := 0; i < reduceNum; i++ {
		task := &TaskInfo{
			TaskId:         baseId + i,
			TaskType:       ReduceTask,
			FilePath:       selectReduceName(files, i),
			NReducer:       c.nReducer,
			AssignedWorker: -1,
			FailedWorkers:  nil,
			TaskState:      Idle,
			StartTime:      time.Time{},
		}
		c.taskMap[baseId+i] = task
		c.reduceTasks <- baseId + i
		fmt.Println("make a reduce task :", c.taskMap[baseId+i])
	}
}

func selectReduceName(files []fs.FileInfo, reduceNum int) []string {
	var s []string
	for _, file := range files {
		if strings.HasPrefix(file.Name(), "mr-") && strings.HasSuffix(file.Name(), strconv.Itoa(reduceNum)) {
			s = append(s, file.Name())
		}
	}
	return s
}
