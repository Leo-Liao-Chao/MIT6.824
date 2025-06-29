package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// Master 定义
type Master struct {
	Mutex           sync.Mutex     // 互斥锁，保护共享资源的并发访问
	MapperFinished  bool           // mapper是否全部完成
	ReducerFinished bool           // reducer是否全部完成
	Mappers         []*MapperTask  // mapper任务列表
	Reducers        []*ReducerTask // reducer任务列表
}

// MapperTask 定义
type MapperTask struct {
	Index        int         // 任务编号
	Assigned     bool        // 是否分配
	AssignedTime time.Time   // 分配时间
	IsFinished   bool        // 是否完成
	InputFile    string      // 输入文件
	ReducerCount int         // 有多少路reducer
	timeoutTimer *time.Timer // 任务超时定时器
}

// ReducerTask 定义
type ReducerTask struct {
	Index        int         // 任务编号
	Assigned     bool        // 是否分配
	AssignedTime time.Time   // 分配时间
	IsFinished   bool        // 是否完成
	MapperCount  int         // 有多少路mapper
	timeoutTimer *time.Timer // 任务超时定时器
}

// func (m *Master) FetchTask(request *)

// Master 是否完成
func (m *Master) Done() bool {
	ret := m.MapperFinished && m.ReducerFinished
	return ret
}

// 初始化Master
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	m.Mutex = sync.Mutex{}
	m.MapperFinished = false
	m.ReducerFinished = false

	m.Mappers = make([]*MapperTask, 0) // 1, Mapper任务列表，数组
	for i, file := range files {
		// := 初始化
		mapper := &MapperTask{
			Index:        i,
			Assigned:     false,       // 是否分配，只是初始化，调用的时候作为分配
			AssignedTime: time.Time{}, // 初始化
			IsFinished:   false,
			InputFile:    file,    // 输入文件
			ReducerCount: nReduce, // 有多少路reducer
			timeoutTimer: nil,     // 任务超时定时器，初始化为nil
		}
		m.Mappers = append(m.Mappers, mapper) // 添加数组
	}

	m.Reducers = make([]*ReducerTask, 0) // 2, Reducer任务列表，数组
	for i := 0; i < nReduce; i++ {
		reducer := &ReducerTask{
			Index:        i,
			Assigned:     false,          // 是否分配
			AssignedTime: time.Time{},    // 初始化
			IsFinished:   false,          // 是否完成
			MapperCount:  len(m.Mappers), // 有多少路mapper
			timeoutTimer: nil,            // 任务超时定时器，初始化为nil
		}
		m.Reducers = append(m.Reducers, reducer) // 添加数组
	}
	m.server()
	return &m
}

// MakeMapperTask函数用于创建Mapper任务
func (m *Master) MakeMapperTask(mapper *MapperTask) {
	mapper.Assigned = true
	mapper.AssignedTime = time.Now()
	// 创建一个定时器，10秒后执行，如果Mapper任务未完成，则将Mapper任务标记为未分配
	mapper.timeoutTimer = time.AfterFunc(10*time.Second, func(index int) func() {
		return func() {
			m.Mutex.Lock()         // 上锁，保护共享资源
			defer m.Mutex.Unlock() // 解锁，确保函数结束时解锁

			if !m.Mappers[index].IsFinished {
				m.Mappers[index].Assigned = false // 如果Mapper任务未完成，则将Mapper任务标记为未分配
			}
		}
	}(mapper.Index))
}

// MakeReducerTask函数用于创建Reducer任务
func (m *Master) MakeReducerTask(reducer *ReducerTask) {
	reducer.Assigned = true
	reducer.AssignedTime = time.Now()
	reducer.timeoutTimer = time.AfterFunc(10*time.Second, func(index int) func() {
		return func() {
			m.Mutex.Lock()         // 上锁，保护共享资源
			defer m.Mutex.Unlock() // 解锁，确保函数结束时解锁

			if !m.Reducers[index].IsFinished {
				m.Reducers[index].Assigned = false // 如果Reducer未完成，则设置Reducer为未分配
			}
		}
	}(reducer.Index))
}

// FinishMapperTask函数用于标记Mapper任务为已完成
func (m *Master) FinishMapperTask(mapper *MapperTask) {
	mapper.IsFinished = true
	mapper.timeoutTimer.Stop() // 停止定时器
}

// FinishReducerTask函数用于标记Reducer任务为已完成
func (m *Master) FinishReducerTask(reducer *ReducerTask) {
	reducer.IsFinished = true
	reducer.timeoutTimer.Stop() // 停止定时器
}

func (m *Master) FetchTask(request *FetchTaskRequest, response *FetchTaskResponse) (err error) {
	m.Mutex.Lock()
	defer m.Mutex.Unlock()
	// 检查所有Mapper任务是否完成
	if !m.MapperFinished {
		for _, mapper := range m.Mappers {
			if mapper.Assigned || mapper.IsFinished {
				continue // 如果任务已经被分配或者已经完成，则跳过
			}
			m.MakeMapperTask(mapper)     // 分配任务
			response.MapperTask = mapper // 返回Mapper任务
			return nil                   // 返回nil表示没有错误
		}
		return nil // 如果没有可分配的Mapper任务，则返回nil
	}
	if !m.ReducerFinished {
		for _, reducer := range m.Reducers {
			if reducer.Assigned || reducer.IsFinished {
				continue // 如果任务已经被分配或者已经完成，则跳过
			}
			m.MakeReducerTask(reducer)     // 分配任务
			response.ReducerTask = reducer // 返回Reducer任务
			return nil                     // 返回nil表示没有错误
		}
		return nil // 如果没有可分配的Reducer任务，则返回nil
	}
	// 如果所有任务都完成了，设置AllFinished为true
	response.AllFinished = true
	return nil
}

// 通知Master任务进度，并且更新Master的Tasks状态
func (m *Master) UpdateTask(request *UpdateTaskRequest, response *UpdateTaskResponse) (err error) {
	m.Mutex.Lock()
	defer m.Mutex.Unlock()

	// 检查Mapper任务是否存在
	if request.Mapper != nil {
		AllMappersFinished := true // 用于检查所有Mapper任务是否完成
		for _, mapper := range m.Mappers {
			if mapper.Index == request.Mapper.Index && !mapper.IsFinished && mapper.Assigned {
				m.FinishMapperTask(mapper) // 回报任务已经完成
			}
			AllMappersFinished = AllMappersFinished && mapper.IsFinished // 检查所有Mapper任务是否完成
		}
		m.MapperFinished = AllMappersFinished // 更新Mapper任务是否完成状态
	}
	if request.Reducer != nil {
		AllReducersFinished := true // 用于检查所有Reducer任务是否完成
		for _, reducer := range m.Reducers {
			if reducer.Index == request.Reducer.Index && !reducer.IsFinished && reducer.Assigned {
				m.FinishReducerTask(reducer) // 回报任务已经完成
			}
			AllReducersFinished = AllReducersFinished && reducer.IsFinished // 检查所有Reducer任务是否完成

		}
		m.ReducerFinished = AllReducersFinished // 更新Reducer任务是否完成状态
	}

	return nil // 返回nil表示没有错误
}

// server函数用于启动Master的RPC服务器
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
