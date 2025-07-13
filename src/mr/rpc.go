package mr

import (
	"os"
	"strconv"
)

type FetchTaskRequest struct {
}
type FetchTaskResponse struct {
	AllFinished bool         // Map和Reduce任务全部完成
	MapperTask  *MapperTask  // mapper任务
	ReducerTask *ReducerTask // reducer任务
}

type UpdateTaskRequest struct {
	Mapper  *MapperTask  // 回报的Mapper任务
	Reducer *ReducerTask // 回报的Reducer任务
}
type UpdateTaskResponse struct {
}

// 创建unix domain socket name
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
