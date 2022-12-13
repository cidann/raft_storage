package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import (
	"fmt"
	"time"
	"sync"
)

type fileTask struct{file string;task int}

type Master struct {
	mapJobs chan fileTask
	reduceJobs chan int
	jobMonitor map[string]chan bool
	mapRemain int
	rangeBound int
	reduceRemain int
	nReduce int
	lock *sync.Cond
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) AskForJob(request *JobRequest, job *JobResponse) error {
	m.lock.L.Lock();
	defer func(){m.lock.L.Unlock();m.lock.Broadcast()}()
	fmt.Println(m,len(m.mapJobs),len(m.reduceJobs))
	
	if m.mapRemain>0{
		select  {

		case mapJob:=<-m.mapJobs:
			fmt.Println("map scheduled")
			job.MapJob=&MapJob{mapJob.file,mapJob.task,m.nReduce}
			waitRes:=make(chan bool,1)
			jobName:=fmt.Sprintf("map-%d",mapJob.task)
			m.jobMonitor[jobName]=waitRes

			go func(){
				m.lock.L.Lock()
				defer func(){delete(m.jobMonitor,jobName);m.lock.L.Unlock();m.lock.Broadcast()}()
				terminate:=time.After(10*time.Second)
				for{
					select{
					case <-m.jobMonitor[jobName]:
						m.mapRemain-=1
						fmt.Println("Map done")
						return
					case <-terminate:	
						fmt.Println("Timedout worker")
						m.mapJobs<-fileTask{mapJob.file,mapJob.task}
						return
					default:
						m.lock.Broadcast()
						m.lock.Wait()
					}
				}
			}()


		default:

		}

	} else if m.reduceRemain>0{
		select  {

		case reduceJob:=<-m.reduceJobs:
			fmt.Println("reduce scheduled",len(m.reduceJobs))
			job.ReduceJob=&ReduceJob{reduceJob,m.rangeBound}
			waitRes:=make(chan bool,1)
			jobName:=fmt.Sprintf("reduce-%d",reduceJob)
			m.jobMonitor[jobName]=waitRes

			go func(){
				m.lock.L.Lock()
				defer func(){delete(m.jobMonitor,jobName);m.lock.L.Unlock();m.lock.Broadcast()}()
				terminate:=time.After(10*time.Second)
				for{
					select{
					case <-m.jobMonitor[jobName]:
						m.reduceRemain-=1
						fmt.Println("Reduce done")
						return
					case <-terminate:
						fmt.Println("Timedout worker")
						m.reduceJobs<-reduceJob
						return
					default:
						m.lock.Broadcast()
						m.lock.Wait()
					}
				}
			}()
		default:

		}

	} else{
		fmt.Println("job done")
		job.JobDone=true
	}

	return nil
}


func (m *Master) ReportDone(report *JobDone, job *JobResponse) error {
	m.lock.L.Lock()
	defer func(){m.lock.L.Unlock();m.lock.Broadcast()}()
	jobName:=fmt.Sprintf("%s-%d",report.JobType,report.TaskDone)
	if _,ok:=m.jobMonitor[jobName];ok{
		m.jobMonitor[jobName]<-true
	}
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	return m.mapRemain==0&&m.reduceRemain==0
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		make(chan fileTask,len(files)),
		make(chan int,nReduce),
		make(map[string]chan bool),
		len(files),
		len(files),
		nReduce,
		nReduce,
		sync.NewCond(&sync.Mutex{}),
	}

	// Your code here.

	for i,v:=range files{
		m.mapJobs<-fileTask{v,i}
	}
	for i:=0;i<nReduce;i++{
		m.reduceJobs<-i
	}
	m.server()
	return &m
}
