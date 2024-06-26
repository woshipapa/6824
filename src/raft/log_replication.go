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

	if rf.Log.empty() {
		DPrintf("Node %d: Log is empty, PrevLogIndex=%d, snapshotLastIncludeIndex=%d", rf.me, args.PrevLogIndex, rf.snapshotLastIncludeIndex)
		// 首先可以确定的是，主结点的args.PrevLogIndex = min(rf.nextIndex[i]-1, rf.lastLogIndex)
		// 这可以比从节点的rf.snapshotLastIncludeIndex大、小或者等价， 因为可以根据
		// args.PrevLogIndex的计算式子得出，nextIndex在leader刚选出时是0，
		// 日志为空，要么是节点刚启动的初始状态，要么是被快照截断后的状态
		// 在初始状态，两者都是0，从节点可以全部接收日志，
		// 若被日志截断，则rf.snapshotLastIncludeIndex前面的日志都是无效的，
		// args.PrevLogIndex >  rf.snapshotLastIncludeIndex 这部分
		// 日志肯定不能插入，所以也会丢弃
		if args.PrevLogIndex == rf.snapshotLastIncludeIndex {
			rf.Log.appendL(args.Logs...)
			reply.FollowerTerm = rf.currentTerm
			reply.Success = true
			//reply. = rf.log.LastLogIndex
			//reply.PrevLogTerm = rf.getLastEntryTerm()
			return
		} else {
			reply.FollowerTerm = rf.currentTerm
			reply.Success = false
			reply.ConflictIndex = rf.Log.LastLogIndex
			reply.ConflictTerm = rf.getLastEntryTerm()
			return
		}
	}

	if args.PrevLogIndex > rf.Log.LastLogIndex {
		//follower的日志短于leader的，这里是follower的日志缺少了miss一部分
		reply.Success = false
		reply.FollowerTerm = rf.currentTerm
		reply.ConflictIndex = rf.Log.LastLogIndex + 1
		reply.ConflictTerm = -1
		return
	}
	if args.PrevLogIndex <= rf.Log.LastLogIndex {
		if args.PrevLogIndex < rf.snapshotLastIncludeIndex {
			DPrintf("Node %d: PrevLogIndex (%d) is less than snapshotLastIncludeIndex (%d)", rf.me, args.PrevLogIndex, rf.snapshotLastIncludeIndex)
			reply.Success = false
			reply.FollowerTerm = rf.currentTerm
			reply.ConflictIndex = rf.snapshotLastIncludeIndex + 1
			reply.ConflictTerm = rf.snapshotLastIncludeTerm
			return
		}
		//这里是follower的日志多了一部分
		if rf.getEntryTerm(args.PrevLogIndex) != args.PrevLogTerm {
			reply.Success = false
			reply.FollowerTerm = rf.currentTerm
			reply.ConflictTerm = rf.getEntryTerm(args.PrevLogIndex)                              //当前follower的最后一条日志条录的term，但是他与当前leader认为相同位置的日志条目term不同
			reply.ConflictIndex = rf.findFirstIndexOfTerm(args.PrevLogIndex, reply.ConflictTerm) //找到这个不符合的term的第一个，可以减少很多的AppendEntries
			//reply.ConflictIndex = args.PrevLogIndex - 1
			if reply.ConflictIndex < rf.Log.FirstLogIndex {
				reply.ConflictIndex = rf.snapshotLastIncludeIndex + 1
			}
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
		} else if index >= rf.snapshotLastIncludeIndex && rf.Log.getOneEntry(index).Term != entry.Term {
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
	if args.LeaderCommit > rf.commitIndex {
		// 输出当前的 lastApplied、LeaderCommit 和 commitIndex
		DPrintf("Before updating commitIndex: Node %d, lastApplied=%d, LeaderCommit=%d, commitIndex=%d, LastLogIndex=%d",
			rf.me, rf.lastApplied, args.LeaderCommit, rf.commitIndex, rf.Log.LastLogIndex)

		// 更新 commitIndex
		rf.commitIndex = min(args.LeaderCommit, rf.Log.LastLogIndex)

		// 输出更新后的 commitIndex
		DPrintf("After updating commitIndex: Node %d, new commitIndex=%d", rf.me, rf.commitIndex)

		if rf.commitIndex > rf.lastApplied {
			// 唤醒每个等待应用日志到状态机的协程
			rf.applyCond.Broadcast()
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
		if rf.Log.empty() { //判掉为空的情况 方便后面讨论
			go rf.sendInstallSnapshot(targetServerId)
			return
		}
		if reply.ConflictIndex < rf.Log.FirstLogIndex {
			go rf.sendInstallSnapshot(targetServerId)

			return
		}
		if term == -1 {
			DPrintf("Follower %d log shorter than expected, adjusting nextIndex to %d", targetServerId, max(1, index))
			//说明follower的日志条目比较短还没有到预期的index，所以下调到follower对应的位置
			rf.nextIndex[targetServerId] = max(1, index) //这里返回的index就是当前follower日志最后的下一个待插入位置
		} else {
			DPrintf("Log inconsistency found at term %d, decrementing nextIndex for follower %d", term, targetServerId)
			//rf.nextIndex[targetServerId] = index
			conflictIndex := -1
			//leader先找，看能不能找到与冲突任期相同的日志，有的话就是返回这个任期的最后一个再去和follower比对
			for i := args.PrevLogIndex; i >= rf.Log.FirstLogIndex; i-- {
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
	return rf.Log.FirstLogIndex
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
		rf.persist() // 可能没必要
		// DPrintf(500, "%v: commitIndex = %v ,entries=%v", rf.SayMeL(), rf.commitIndex, rf.log.Entries)
		DPrintf("%v: 主结点已经提交了index为%d的日志，rf.lastApplied=%v rf.commitIndex=%v", rf.SayMeL(), rf.commitIndex, rf.lastApplied, rf.commitIndex)
		rf.applyCond.Broadcast() // 通知每个节点的协程去检查当前commitIndex,但实际上这里只有leader的协程会奏效
	} else {
		DPrintf("\n%v: 未超过半数节点在此索引index : %d 上的日志相等，拒绝提交....\n", rf.SayMeL(), matchIndex)
	}

}
