package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
)

type KeyValue struct {
	Key   string
	Value string
}
type ArrayKeyValue []KeyValue

func (a ArrayKeyValue) Len() int           { return len(a) }
func (a ArrayKeyValue) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ArrayKeyValue) Less(i, j int) bool { return a[i].Key < a[j].Key }

func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func CallFetchTask() (ret *FetchTaskResponse) {
	request := FetchTaskRequest{}
	response := FetchTaskResponse{}
	// 调用Master的FecthTask RPC方法，rpc包实现的
	if call("Master.FetchTask", &request, &response) {
		ret = &response
	}
	return ret
}

func CallUpdateTaskForMapper(mapper *MapperTask) {
	request := UpdateTaskRequest{Mapper: mapper}
	response := UpdateTaskResponse{}
	// 调用Master的UpdateTask RPC方法，rpc包实现的
	call("Master.UpdateTask", &request, &response)
}

func CallUpdateTaskForReducer(reducer *ReducerTask) {
	request := UpdateTaskRequest{Reducer: reducer}
	response := UpdateTaskResponse{}
	// 调用Master的UpdateTask RPC方法，rpc包实现的
	call("Master.UpdateTask", &request, &response)
}

func doMapperTask(mapper *MapperTask, mapf func(string, string) []KeyValue) {

	// 读取输入文件
	file, err := os.Open(mapper.InputFile)
	if err != nil {
		log.Fatalf("cannot open %v", mapper.InputFile)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", mapper.InputFile)
	}
	file.Close()

	// 执行map函数
	kva := mapf(mapper.InputFile, string(content))
	// kva是一个KeyValue数组，表示map函数的输出,[key, value]对的数组

	// 按照key进行分组
	reducerArray := make([][]KeyValue, mapper.ReducerCount)
	for _, kv := range kva {
		reducerNum := ihash(kv.Key) % mapper.ReducerCount // 计算reducer编号
		reducerArray[reducerNum] = append(reducerArray[reducerNum], kv)
	}

	// reducerArray是一个二维数组，表示每个reducer对应的KeyValue数组
	for i, kvs := range reducerArray {
		sort.Sort(ArrayKeyValue(kvs)) // 对每个Reducer的KeyValue数组进行排序

		filename := fmt.Sprintf("mr-%d-%d", mapper.Index, i)
		ofile, err := os.Create(filename + ".tmp")
		if err != nil {
			log.Fatalf("cannot open %v", filename+".tmp")
		}

		enc := json.NewEncoder(ofile)
		for _, kv := range kvs {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("cannot jsonencode %v", filename+".tmp")
			}
		}
		ofile.Close()
		// 写完后原子重命名为 mr-X-Y，确保写入完整性（防止崩溃写一半）。
		os.Rename(filename+".tmp", filename) // 重命名文件，完成输出
	}

	// 通知Master任务完成
	CallUpdateTaskForMapper(mapper)
}

func doReducerTask(reducer *ReducerTask, reducef func(string, []string) string) {
	kvs := make(ArrayKeyValue, 0)

	for i := 0; i < reducer.MapperCount; i++ {
		filename := fmt.Sprintf("mr-%d-%d", i, reducer.Index)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break // 读取完毕
			}
			kvs = append(kvs, kv) // 将读取的KeyValue添加到kvs数组中
		}
	}

	sort.Sort(ArrayKeyValue(kvs)) // 对kvs进行排序

	outputfilename := fmt.Sprintf("mr-out-%d", reducer.Index)
	outputfile, err := os.Create(outputfilename + ".tmp")
	if err != nil {
		log.Fatalf("cannot open %v", outputfilename+".tmp")
	}

	i := 0
	for i < len(kvs) {
		j := i + 1
		for j < len(kvs) && kvs[j].Key == kvs[i].Key {
			j++ // 找到相同key的范围
		}
		// 初始化values切片
		values := make([]string, 0)
		for k := i; k < j; k++ {
			values = append(values, kvs[k].Value) // 收集相同key的所有value
		}
		output := reducef(kvs[i].Key, values)                  // 调用reduce函数处理
		fmt.Fprintf(outputfile, "%v %v\n", kvs[i].Key, output) // 写入结果
		i = j                                                  // 移动到下一个不同的key
	}
	outputfile.Close()                               // 关闭输出文件
	os.Rename(outputfilename+".tmp", outputfilename) // 重命名输出文件，确保写入完整性
	CallUpdateTaskForReducer(reducer)

}

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		responce := CallFetchTask()

		if responce == nil {
			continue
		}

		if responce.AllFinished {
			return // 所有任务完成，退出
		}

		if responce.MapperTask != nil {
			doMapperTask(responce.MapperTask, mapf) // 执行Mapper任务
		}

		if responce.ReducerTask != nil {
			doReducerTask(responce.ReducerTask, reducef) // 执行Reducer任务
		}
	}
}

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
