package raft

import "sort"

/*
*
接收方收到日志更新的RPC
*/
func (rf *Raft) HandleAppendEntriesRPC(args *AppendEntriesArgs, reply *AppendEntriesReply) {
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
		if args.PrevLogIndex != -1 && rf.Log.Entries[args.PrevLogIndex].Term != args.PrevLogTerm {
			reply.Success = false
			reply.FollowerTerm = rf.currentTerm
			reply.ConflictTerm = rf.Log.Entries[args.PrevLogIndex].Term //当前follower的最后一条日志条录的term，但是他与当前leader认为相同位置的日志条目term不同
			//reply.ConflictIndex = rf.findFirstIndexOfTerm(reply.ConflictTerm) //找到这个不符合的term的第一个，可以减少很多的AppendEntries
			reply.ConflictIndex = args.PrevLogIndex - 1
			DPrintf("Node %d log inconsistency at index %d; found term %d, expected %d", rf.me, args.PrevLogIndex, reply.ConflictTerm, args.PrevLogTerm)
			return
		}
	}

	// [PrevLogIndex+1,...]接受最新的log
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Success = true
	reply.FollowerTerm = rf.currentTerm
	//可能日志后面有冲突的，先把后面的都清掉
	rf.Log.Entries = rf.Log.Entries[:args.PrevLogIndex+1]
	//加入收到的leader发来的新日志
	rf.Log.Entries = append(rf.Log.Entries, args.Logs...)
	rf.Log.LastLogIndex = len(rf.Log.Entries) - 1
	DPrintf("Node %d appended new entries from leader %d; last log index now %d", rf.me, args.LeaderId, rf.Log.LastLogIndex)
	if args.LeaderCommit > rf.commitIndex {
		//说明该把 [rf.lastapplied,args.LeaderCommit]这部分的指令去应用到状态机中
		rf.commitIndex = args.LeaderCommit
		rf.applyLogs()
	}
}

func (rf *Raft) applyLogs() {
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      rf.Log.Entries[rf.lastApplied].Command,
			CommandIndex: rf.lastApplied,
		}
		rf.applyCh <- applyMsg
	}
}
func (rf *Raft) handleAppendEntriesReply(targetServerId int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//rf.mu.Lock()
	//defer rf.mu.Unlock()

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
		DPrintf("AppendEntries from leader %d to follower %d was successful, updating nextIndex to %d and matchIndex to %d", rf.me, targetServerId, args.PrevLogIndex+1+len(args.Logs), args.PrevLogIndex+len(args.Logs))
		// AppendEntries请求成功，更新nextIndex和matchIndex
		rf.nextIndex[targetServerId] = args.PrevLogIndex + 1 + len(args.Logs)
		rf.matchIndex[targetServerId] = rf.nextIndex[targetServerId] - 1
		rf.updateCommitIndex()
	} else {
		index := reply.ConflictIndex
		term := reply.ConflictTerm
		if term == -1 {
			DPrintf("Follower %d log shorter than expected, adjusting nextIndex to %d", targetServerId, max(1, index))
			//说明follower的日志条目比较短还没有到预期的index，所以下调到follower对应的位置
			rf.nextIndex[targetServerId] = max(1, index)
		} else {
			DPrintf("Log inconsistency found at term %d, decrementing nextIndex for follower %d", term, targetServerId)
			rf.nextIndex[targetServerId]--
			//found := false
			//for i := args.PrevLogIndex; i >= rf.Log.FirstLogIndex; i-- {
			//	if rf.Log.Entries[i].Term == term {
			//		//如果冲突任期存在，查找该任期在日志中的最后一个索引，并更新nextIndex。
			//		found = true
			//		rf.nextIndex[targetServerId] = i + 1 //下次发送的prevLogIndex和这个follower最后一条日志的term就相同了
			//		break
			//	}
			//}
			//if !found {
			//	rf.nextIndex[targetServerId] = index
			//}
		}

	}

}
func (rf *Raft) updateCommitIndex() {
	// 复制一个matchIndex的副本进行排序，以便不阻塞其他操作
	matchCopy := append([]int(nil), rf.matchIndex...)
	sort.Ints(matchCopy)
	N := matchCopy[len(matchCopy)/2] // 获得中位数，即多数派已经复制的最高日志索引

	// 检查N是否大于commitIndex且N对应的日志条目任期等于currentTerm
	// 只有在当前任期的日志条目可以被提交
	if N > rf.commitIndex && rf.Log.Entries[N].Term == rf.currentTerm {
		rf.commitIndex = N
		rf.applyLogs() // 应用到状态机
	}
}
func (rf *Raft) findFirstIndexOfTerm(term int) int {
	for i := len(rf.Log.Entries) - 1; i >= 0; i-- {
		if rf.Log.Entries[i].Term != term {
			return i + 1
		}
	}
	return -1
}
