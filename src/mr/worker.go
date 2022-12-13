package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "time"
import "io/ioutil"
import "sort"
import "encoding/json"
import "os"

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
type ByKey []KeyValue
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for{
		response:=RequestResponse()
		if response.JobDone{
			return
		} else if response.MapJob!=nil{
			filename:=response.MapJob.Filename
			taskNum:=response.MapJob.TaskNum
			nReduce:=response.MapJob.NReduce

			intermediate := make(map[int][]KeyValue)
			
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()
			kva := mapf(filename, string(content))
			for _,i:=range kva{
				k:=i.Key
				hash:=ihash(k)%nReduce
				_,ok:=intermediate[hash]
				if !ok{
					intermediate[hash]=make([]KeyValue,0)
				} 
				intermediate[hash]=append(intermediate[hash],i)
			}

			for r:=0;r<nReduce;r++{
				oname := fmt.Sprintf("mr-%d-%d",taskNum,r)
				file,_:=os.Create(oname)
				enc := json.NewEncoder(file)
				if _,ok:=intermediate[r];!ok{
					continue
				}
				for _,kvs:=range intermediate[r]{
					if writeErr := enc.Encode(&kvs); writeErr != nil {
						fmt.Println("writeErr")
						return
					}
				}
				
			}

			TellDone("map",taskNum)

		} else if response.ReduceJob!=nil{
			rangeBound:=response.ReduceJob.RangeBound
			reduceNum:=response.ReduceJob.ReduceNum
			
			intermediate := []KeyValue{}
			for r:=0;r<rangeBound;r++{
				filename := fmt.Sprintf("mr-%d-%d",r,reduceNum)
				file, openErr := os.Open(filename)
				if openErr != nil {
					log.Fatalf("cannot open %v", filename)
				}
				dec := json.NewDecoder(file)
				for{
					var kv KeyValue
					if readErr := dec.Decode(&kv); readErr != nil {
						break
					}
					
					intermediate = append(intermediate, kv)
				}
				file.Close()

			}
			sort.Sort(ByKey(intermediate))
			oname := fmt.Sprintf("mr-out-%d",reduceNum)
			ofile, _ := os.Create(oname)

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

			TellDone("reduce",reduceNum)

		} else{
			time.Sleep(10*time.Second)
		}
	}
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func RequestResponse() *JobResponse{

	// declare an argument structure.
	args := JobRequest{}

	// declare a reply structure.
	reply := JobResponse{}

	call("Master.AskForJob", &args, &reply)
	return &reply
}

func TellDone(jobType string,taskDone int){
	// declare an argument structure.
	args := JobDone{jobType,taskDone}

	// declare a reply structure.
	reply := JobResponse{}

	call("Master.ReportDone", &args, &reply)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply *JobResponse) bool {
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
