package raft

type RequestInstallSnapShotArgs struct {
	Term             int
	LeaderId         int
	LastIncludeIndex int
	LastIncludeTerm  int
	Snapshot         []byte
}
type RequestInstallSnapShotReply struct {
	Term int
}

func (rf *Raft) InstallSnapShot(args RequestInstallSnapShotArgs, reply *RequestInstallSnapShotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//defer DPrintf(11, "%v: RequestInstallSnapshot end  args.LeaderId=%v, args.LastIncludeIndex=%v, args.LastIncludeTerm=%v\n", rf.SayMeL(), args.LeaderId, args.LastIncludeIndex, args.LastIncludeTerm)
	DPrintf("%v: receiving snapshot from LeaderId=%v with args.LastIncludeIndex=%v, args.LastIncludeTerm=%v\n", rf.SayMeL(), args.LeaderId, args.LastIncludeIndex, args.LastIncludeTerm)
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		DPrintf("%v: refusing snapshot from leader %d 's snapshot request since its term is %d", rf.SayMeL(), args.LeaderId, args.Term)
		return
	}
	rf.state = Follower
	rf.resetElectionTimer()
	if args.Term > rf.currentTerm {
		rf.votedFor = -1
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
	}
	defer rf.persist()
	if args.LastIncludeIndex > rf.snapshotLastIncludeIndex {
		//DPrintf( "%v: before install snapshot from leader %d: leader.log=%v", rf.SayMeL(), args.LeaderId, rf.log)
		rf.snapshot = args.Snapshot
		rf.snapshotLastIncludeIndex = args.LastIncludeIndex
		rf.snapshotLastIncludeTerm = args.LastIncludeTerm

		// 对于follower日志的截断
		if args.LastIncludeIndex >= rf.Log.LastLogIndex {
			rf.Log.Entries = make([]Entry, 0) //快照已经遥遥领先于当前节点的日志，可以忽略所有日志了
			rf.Log.LastLogIndex = args.LastIncludeIndex
		} else {
			rf.Log.Entries = rf.Log.Entries[rf.Log.getRealIndex(args.LastIncludeIndex+1):]
		}
		rf.Log.FirstLogIndex = args.LastIncludeIndex + 1

		DPrintf("%v: after install snapshot rf.log.FirstLogIndex=%v, rf.log=%v", rf.SayMeL(), rf.Log.FirstLogIndex, rf.Log.Entries)

		if args.LastIncludeIndex > rf.lastApplied {
			//该节点应用落后于leader的日志部分
			msg := ApplyMsg{
				SnapshotValid: true,
				Snapshot:      rf.snapshot,
				SnapshotTerm:  rf.snapshotLastIncludeTerm,
				SnapshotIndex: rf.snapshotLastIncludeIndex,
			}
			DPrintf("%v: next apply snapshot rf.snapshot.LastIncludeIndex=%v rf.snapshot.LastIncludeTerm=%v\n", rf.SayMeL(), rf.snapshotLastIncludeIndex, rf.snapshotLastIncludeTerm)
			rf.ApplyHelper.tryApply(&msg)
			rf.lastApplied = args.LastIncludeIndex
		}
		rf.commitIndex = max(rf.commitIndex, args.LastIncludeIndex)
	}
	DPrintf("%v: successfully installing snapshot from LeaderId=%v with args.LastIncludeIndex=%v, args.LastIncludeTerm=%v\n", rf.SayMeL(), args.LeaderId, args.LastIncludeIndex, args.LastIncludeTerm)
}

func (rf *Raft) sendInstallSnapshot(serverId int) {
	args := RequestInstallSnapShotArgs{}
	reply := RequestInstallSnapShotReply{}
	rf.mu.Lock()
	if rf.state != Leader {
		DPrintf("%v: 状态已变，不是leader节点，无法发送快照", rf.SayMeL())
		rf.mu.Unlock()
		return
	}
	DPrintf("%v: 准备向节点%d发送快照", rf.SayMeL(), serverId)
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.LastIncludeIndex = rf.snapshotLastIncludeIndex
	args.LastIncludeTerm = rf.snapshotLastIncludeTerm
	args.Snapshot = rf.snapshot
	rf.mu.Unlock()

	//发出RPC请求
	ok := rf.sendRequestInstallSnapshot(serverId, &args, &reply)

	if !ok {
		return
	}

}
func (rf *Raft) sendRequestInstallSnapshot(server int, args *RequestInstallSnapShotArgs, reply *RequestInstallSnapShotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}
