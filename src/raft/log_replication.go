package raft

import "fmt"

/*
*
接收方收到日志更新的RPC
*/
func (rf *Raft) HandleAppendEntriesRPC(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		//过时的leader发来请求
		reply.Success = false
		reply.FollowerTerm = rf.currentTerm
		return
	}
	// 如果term大于当前term，更新当前term并转换为follower
	if args.Term > rf.currentTerm {
		// Term 与 当前leader不同，说明他没有投票给当前leader，说明当前follower错过了这个leader的选举
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	rf.state = Follower
	// 重置选举计时器，因为收到了有效的AppendEntries RPC
	DPrintf("follower %d 收到了 leader的日志复制请求，进行选举时间的刷新", rf.me)
	rf.resetElectionTimer()

	defer rf.persist()

	if args.PrevLogIndex > rf.Log.LastLogIndex {
		//follower的日志短于leader的，这里是follower的日志缺少了miss一部分
		reply.Success = false
		reply.FollowerTerm = rf.currentTerm
		reply.ConflictIndex = rf.Log.LastLogIndex + 1
		reply.ConflictTerm = -1
		return
	}
	if args.PrevLogIndex <= rf.Log.LastLogIndex {

		//这里是follower的日志多了一部分
		if rf.getEntryTerm(args.PrevLogIndex) != args.PrevLogTerm {
			reply.Success = false
			reply.FollowerTerm = rf.currentTerm
			reply.ConflictTerm = rf.getEntryTerm(args.PrevLogIndex)                              //当前follower的最后一条日志条录的term，但是他与当前leader认为相同位置的日志条目term不同
			reply.ConflictIndex = rf.findFirstIndexOfTerm(args.PrevLogIndex, reply.ConflictTerm) //找到这个不符合的term的第一个，可以减少很多的AppendEntries
			//reply.ConflictIndex = args.PrevLogIndex - 1
			DPrintf("Node %d log inconsistency at index %d; found term %d, expected %d", rf.me, args.PrevLogIndex, reply.ConflictTerm, args.PrevLogTerm)
			return
		}
	}

	// [PrevLogIndex+1,...]接受最新的log

	reply.Success = true
	reply.FollowerTerm = rf.currentTerm
	ok := true
	for i, entry := range args.Logs {
		index := args.PrevLogIndex + i + 1
		if index > rf.Log.LastLogIndex {
			rf.Log.appendL(entry)
			DPrintf("Node %d appended new entry at index %d: %v", rf.me, index, entry.Command)
		} else if rf.Log.getOneEntry(index).Term != entry.Term {
			ok = false
			DPrintf("Node %d found term mismatch at index %d: 本来Log的term : %d, entry :  %d", rf.me, index, rf.Log.getOneEntry(index).Term, rf.Log.getOneEntry(index).Command)
			//rf.Log.Entries[index-1] = entry // 覆盖
			*rf.Log.getOneEntry(index) = entry
			DPrintf("Node %d overwritten entry at index %d with: %v", rf.me, index, entry.Command)
			// 打印整个日志列表
			//DPrintf("Node %d 现在的日志列表:", rf.me)
			//for i, logEntry := range rf.Log.Entries {
			//	DPrintf("Index: %d, Term: %d, Command: %v", i+1, logEntry.Term, logEntry.Command)
			//}
		}
	}
	if !ok {
		rf.Log.LastLogIndex = args.PrevLogIndex + len(args.Logs)
	}
	//follower去将日志更新到状态机中去
	if args.LeaderCommit > rf.commitIndex {
		//说明该把 [rf.lastapplied,args.LeaderCommit]这部分的指令去应用到状态机中
		rf.commitIndex = min(args.LeaderCommit, rf.Log.LastLogIndex)
		if rf.commitIndex > rf.lastApplied {
			rf.applyCond.Broadcast() //唤醒每个follower等待应用日志到状态机的协程 sendMsgToTester
		}
	}
}

//	func (rf *Raft) applyLogs() {
//		for rf.lastApplied < rf.commitIndex {
//			rf.lastApplied++
//			applyMsg := ApplyMsg{
//				CommandValid: true,
//				Command:      rf.Log.Entries[rf.lastApplied].Command,
//				CommandIndex: rf.lastApplied,
//			}
//			rf.applyCh <- applyMsg
//		}
//	}
func (rf *Raft) handleAppendEntriesReply(targetServerId int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	// 死锁了这里

	if rf.state != Leader {
		return
	}

	if reply.FollowerTerm > rf.currentTerm {
		rf.state = Follower
		rf.votedFor = -1
		rf.currentTerm = reply.FollowerTerm
		rf.persist()
		return
	}

	if reply.Success {
		rf.nextIndex[targetServerId] = args.PrevLogIndex + 1 + len(args.Logs)
		rf.matchIndex[targetServerId] = rf.nextIndex[targetServerId] - 1
		fmt.Printf("AppendEntries to %d succeeded: nextIndex[%d] = %d, matchIndex[%d] = %d\n",
			targetServerId, targetServerId, rf.nextIndex[targetServerId], targetServerId, rf.matchIndex[targetServerId])

		rf.tryCommit(rf.matchIndex[targetServerId])
		//if reply.FollowerTerm == rf.currentTerm && *appendNums <= len(rf.peers)/2 {
		//	(*appendNums)++
		//	DPrintf("Leader %d received successful AppendEntries reply from follower %d; incremented success count to %d", rf.me, targetServerId, *appendNums)
		//} else {
		//	DPrintf("Leader %d received successful AppendEntries reply from follower %d but either term mismatch or majority already achieved", rf.me, targetServerId)
		//}
		//
		//DPrintf("Leader %d updated nextIndex to %d and matchIndex to %d for follower %d after successful AppendEntries", rf.me, rf.nextIndex[targetServerId], rf.matchIndex[targetServerId], targetServerId)
		//
		//if *appendNums > len(rf.peers)/2 {
		//	*appendNums = 0
		//	for rf.lastApplied < len(rf.Log.Entries)-1 {
		//		rf.lastApplied++
		//		applyMsg := ApplyMsg{
		//			CommandValid: true,
		//			Command:      rf.Log.Entries[rf.lastApplied].Command,
		//			CommandIndex: rf.lastApplied,
		//		}
		//		rf.applyCh <- applyMsg
		//		rf.commitIndex = rf.lastApplied
		//		//fmt.Printf("[	sendAppendEntries func-rf(%v)	] commitLog  \n", rf.me)
		//	}
		//	DPrintf("Leader %d has achieved majority of successful AppendEntries, applying logs to state machine", rf.me)
		//}

	} else {
		index := reply.ConflictIndex
		term := reply.ConflictTerm
		if term == -1 {
			DPrintf("Follower %d log shorter than expected, adjusting nextIndex to %d", targetServerId, max(1, index))
			//说明follower的日志条目比较短还没有到预期的index，所以下调到follower对应的位置
			rf.nextIndex[targetServerId] = max(1, index) //这里返回的index就是当前follower日志最后的下一个待插入位置
		} else {
			DPrintf("Log inconsistency found at term %d, decrementing nextIndex for follower %d", term, targetServerId)
			//rf.nextIndex[targetServerId] = index
			conflictIndex := -1
			//leader先找，看能不能找到与冲突任期相同的日志，有的话就是返回这个任期的最后一个再去和follower比对
			for i := args.PrevLogIndex; i > 0; i-- {
				if rf.getEntryTerm(i) == reply.ConflictTerm {
					conflictIndex = i
					break
				}
			}
			if conflictIndex != -1 {
				rf.nextIndex[targetServerId] = conflictIndex + 1
				//下次leader在去和这个follower联系时，他们比较的就是相同任期的日志了
			} else {
				//这里说明leader中不含这个任期的日志，follower中这些任期的都不要了，所以从follower这些任期的第一个开始
				rf.nextIndex[targetServerId] = reply.ConflictIndex
			}
		}

	}

}

func (rf *Raft) findFirstIndexOfTerm(preLogIndex int, term int) int {
	for i := preLogIndex; i >= rf.Log.FirstLogIndex; i-- {
		if rf.Log.getOneEntry(i).Term != term {
			return i + 1
		}
	}
	return 1
}

func (rf *Raft) tryCommit(matchIndex int) {
	if matchIndex <= rf.commitIndex || matchIndex < rf.Log.FirstLogIndex || matchIndex > rf.Log.LastLogIndex {
		return
	}

	//if rf.getEntryTerm(matchIndex) != rf.currentTerm {
	//	// 提交的必须本任期内从客户端收到的日志
	//	return
	//}
	// 计算所有已经正确匹配该matchIndex的从节点的票数
	cnt := 1 //自动计算上leader节点的一票
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		// 为什么只需要保证提交的matchIndex必须小于等于其他节点的matchIndex就可以认为这个节点在这个matchIndex记录上正确匹配呢？
		// 因为matchIndex是增量的，如果一个从节点的matchIndex=10，则表示该节点从1到10的子日志都和leader节点对上了
		if matchIndex <= rf.matchIndex[i] {
			cnt++
		}
	}

	if cnt > len(rf.peers)/2 {
		rf.commitIndex = matchIndex //leader已经可以提交matchIndex这里的日志了，半数以上投票了
		//发生事件 leader 的 commitIndex更新了
		if rf.commitIndex > rf.Log.LastLogIndex {
			DPrintf("%v: commitIndex > lastlogindex %v > %v", rf.SayMeL(), rf.commitIndex, rf.Log.LastLogIndex)
			panic("")
		}
		// DPrintf(500, "%v: commitIndex = %v ,entries=%v", rf.SayMeL(), rf.commitIndex, rf.log.Entries)
		DPrintf("%v: 主结点已经提交了index为%d的日志，rf.lastApplied=%v rf.commitIndex=%v", rf.SayMeL(), rf.commitIndex, rf.lastApplied, rf.commitIndex)
		rf.applyCond.Broadcast() // 通知每个节点的协程去检查当前commitIndex,但实际上这里只有leader的协程会奏效
	} else {
		DPrintf("\n%v: 未超过半数节点在此索引index : %d 上的日志相等，拒绝提交....\n", rf.SayMeL(), matchIndex)
	}

}
