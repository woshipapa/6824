package raft

import (
	"fmt"
	"sync"
	"sync/atomic"
)

// Debugging
const Debug = true

func ifCond(cond bool, a, b interface{}) interface{} {
	if cond {
		return a
	} else {
		return b
	}
}
func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		fmt.Printf(format, a...)
		fmt.Println()
	}
	return
}

type ApplyHelper struct {
	applyCh       chan ApplyMsg
	lastItemIndex int
	q             []ApplyMsg
	mu            sync.Mutex
	cond          *sync.Cond
	dead          int32
}

func (applyHelper *ApplyHelper) Kill() {
	atomic.StoreInt32(&applyHelper.dead, 1)
}
func (applyHelper *ApplyHelper) killed() bool {
	z := atomic.LoadInt32(&applyHelper.dead)
	return z == 1
}
func NewApplyHelper(applyCh chan ApplyMsg, lastApplied int) *ApplyHelper {
	applyHelper := &ApplyHelper{
		applyCh:       applyCh,
		lastItemIndex: lastApplied,
		q:             make([]ApplyMsg, 0),
	}
	applyHelper.cond = sync.NewCond(&applyHelper.mu)
	go applyHelper.applier()
	return applyHelper
}
func (applyHelper *ApplyHelper) applier() {
	for !applyHelper.killed() {
		applyHelper.mu.Lock()
		//defer applyHelper.mu.Unlock()
		//消费者模型，把缓冲队列中的msg取出应用到applyCh中去
		if len(applyHelper.q) == 0 {
			applyHelper.cond.Wait()
		}
		msg := applyHelper.q[0]
		applyHelper.q = applyHelper.q[1:]
		applyHelper.mu.Unlock()
		DPrintf("applyhelper start apply msg index=%v", ifCond(msg.CommandValid, msg.CommandIndex, msg.SnapshotIndex))
		applyHelper.applyCh <- msg
		//<-applyHelper.applyCh
		DPrintf("applyhelper done apply msg index=%v with log entry command : %v", ifCond(msg.CommandValid, msg.CommandIndex, msg.SnapshotIndex), msg.Command)
	}
}

// tryApply() 方法用于尝试将一个新的 ApplyMsg 添加到 ApplyHelper 的内部队列 q 中
func (applyHelper *ApplyHelper) tryApply(msg *ApplyMsg) bool {
	applyHelper.mu.Lock()
	defer applyHelper.mu.Unlock()
	//DPrintf("applyhelper get msg index=%v", ifCond(msg.CommandValid, msg.CommandIndex, msg.SnapshotIndex))
	if msg.CommandValid {
		if msg.CommandIndex <= applyHelper.lastItemIndex {
			return true
		}
		if msg.CommandIndex == applyHelper.lastItemIndex+1 {
			applyHelper.q = append(applyHelper.q, *msg)
			applyHelper.lastItemIndex++
			applyHelper.cond.Broadcast()
			DPrintf("applyhelper added command msg to queue and broadcasted with index=%v and command = %v", msg.CommandIndex, msg.Command)
			return true
		}
		panic("applyhelper meet false")
		return false
	} else if msg.SnapshotValid {
		if msg.SnapshotIndex <= applyHelper.lastItemIndex {
			return true
		}
		applyHelper.q = append(applyHelper.q, *msg)
		applyHelper.lastItemIndex = msg.SnapshotIndex
		applyHelper.cond.Broadcast()
		return true
	} else {
		panic("applyHelper meet both invalid")
	}
}
