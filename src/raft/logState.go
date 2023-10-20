package raft

/*
import (
	//"fmt"
	"bytes"
	"encoding/gob"
)

type LogState struct{
	log [] *ApplyMsg //each entry contains command for state machine, and term when entry was received by leader (first index is 1)
	snapshot *Snapshot
}

type Snapshot struct{
	LastIndex int
	LastTerm int
	SnapBytes []byte
	AppliedBytes []byte
}

var LowerBoundEntry=&ApplyMsg{
	CommandValid:false,
	CommandIndex:-2,
	CommandTerm:-2,
}
var UpperBoundEntry=&ApplyMsg{
	CommandValid:false,
	CommandIndex:-1,
	CommandTerm:-1,
}

func (ls *LogState) logIndex(index int)int{
	return index-ls.snapshot.LastIndex-1
}

func (ls *LogState) GetEntry(index int)*ApplyMsg{
	logIndex:=ls.logIndex(index)
	if index<0{
		return LowerBoundEntry
	} else if index>=ls.Len(){
		return UpperBoundEntry
	} else if logIndex<0{
		return &ApplyMsg{
			CommandValid:false,
			CommandIndex:ls.snapshot.LastIndex,
			CommandTerm:ls.snapshot.LastTerm,
		}
	}
	return ls.log[logIndex]
}

func (ls *LogState) SetEntry(index int,val *ApplyMsg){
	logIndex:=ls.logIndex(index)
	ls.log[logIndex]=val
}

func (ls *LogState) GetLog()[]*ApplyMsg{
	return ls.log
}

func (ls *LogState) SetLog(log []*ApplyMsg){
	ls.log=log
}

func (ls *LogState) SliceLog(start,end int)[]*ApplyMsg{
	logStart,logEnd:=ls.logIndex(start),ls.logIndex(end)
	if logStart<0{
		logStart=0
	}
	if logEnd>=len(ls.log){
		logEnd=len(ls.log)
	}
	return ls.log[logStart:logEnd]
}

func (ls *LogState) ExtendLog(entries ...*ApplyMsg){
	ls.log=append(ls.log,entries...)
}

func (ls *LogState) GetPrevTermIndex(term int)int{
	index:=len(ls.log)-1
	for ;index>0&&ls.log[index].CommandTerm>=term;index--{}
	return index
}

func (ls *LogState) Len()int{
	return ls.snapshot.LastIndex+1+len(ls.log)
}

func (ls *LogState) GetSnapshot()*Snapshot{
	return ls.snapshot
}

func (ls *LogState) SetSnapshot(lastIndex,lastTerm int,snapBytes []byte,appliedBytes []byte){
	newStartIndex:=ls.logIndex(lastIndex)+1
	if newStartIndex>=len(ls.log){
		newStartIndex=len(ls.log)
	}
	ls.log=ls.log[newStartIndex:]
	newSnapshot:=&Snapshot{lastIndex,lastTerm,snapBytes,appliedBytes}
	ls.snapshot=newSnapshot
}


func DeepCopyLog(oldLog []*ApplyMsg)[]*ApplyMsg{
	newLog:=make([]*ApplyMsg,len(oldLog))
	for i,oldEntry:=range oldLog{
		newEntry:=*oldEntry
		newLog[i]=&newEntry
	}
	return newLog
}


func SafeSlice(slice []*ApplyMsg,start,end int)[]*ApplyMsg{
	if start<0{
		start=0
	}else if start>=len(slice){
		start=len(slice)
	}
	if end<0{
		end=0
	}else if end>=len(slice){
		end=len(slice)
	}
	return slice[start:end]
}

func SafeGetEntry(slice []*ApplyMsg,index int)*ApplyMsg{
	if index<0{
		return LowerBoundEntry
	} else if index>=len(slice){
		return UpperBoundEntry
	} else{
		return slice[index]
	}
}

func NewLogState(log []*ApplyMsg,snapshot *Snapshot)*LogState{
	logState:=&LogState{}
	logState.snapshot=snapshot
	logState.log=log
	return logState
}

func ZipBytes(allBytes ...[]byte)[]byte{
	w:=new(bytes.Buffer)
	e:=gob.NewEncoder(w)
	for _,b:=range allBytes{
		e.Encode(b)
	}
	return w.Bytes()
}

func UnzipBytes(zipped []byte,num int)[][]byte{
	r:=bytes.NewBuffer(zipped)
	d:=gob.NewDecoder(r)
	parts:=make([][]byte,num)
	for i:=0;i<num;i++{
		var part []byte
		if d.Decode(&part)!=nil{
			DPrintf("Loading Error\n")
		} else{
			parts[i]=part
		}
	}
	return parts
}
*/
