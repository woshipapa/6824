package raft

import (
	"math/rand"
	"time"
)

func (rf *Raft) pastElectionTimeOut() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return time.Since(rf.lastElection) > rf.electionTimeout
}

func (rf *Raft) pastHeartBeatTimeOut() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return time.Since(rf.lastHeartbeat) > rf.heartbeatTimeout
}

func (rf *Raft) resetHeartBeatTimeOut() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.lastHeartbeat = time.Now()
}

func (rf *Raft) StartElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//开启一轮选举，并重置自己的超时时间
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.resetElectionTimer() //开始新一轮选举时，重置自己的选举超时时间
	DPrintf("[%d] attempting an election at term %d...\n", rf.me, rf.currentTerm)

	//开始拉票
	args := RequestVoteArgs{}
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogTerm = rf.getLastEntryTerm()
	args.LastLogIndex = rf.Log.LastLogIndex
	votes := 1

	for i, _ := range rf.peers {
		if rf.me == i {
			continue
		}
		go func(serverId int) {
			var reply RequestVoteReply
			ok := rf.sendRequestVote(serverId, &args, &reply)
			if !ok || !reply.VoteGranted {
				DPrintf("%v: cannot be given a vote by node %v at reply.term=%v\n", rf.SayMeL(), serverId, reply.PeerTerm)
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			DPrintf("%v: now receiving a vote from %d with term %d\n", rf.SayMeL(), serverId, reply.PeerTerm)
			if reply.PeerTerm < rf.currentTerm {
				DPrintf("%v: 来自%d 在任期 %d 的旧投票，拒绝接受\n", rf.SayMeL(), serverId, reply.PeerTerm)
				return
			} else if reply.PeerTerm > rf.currentTerm {
				DPrintf("%v: %d 的任期是 %d, 比我大，变为follower\n", rf.SayMeL(), serverId, args.Term)
				rf.state = Follower
				rf.votedFor = -1
				rf.currentTerm = reply.PeerTerm
				rf.persist()
				return
			}

			votes++
			//下面这个state的判断可以防止当选票超过半数以上时，重复的执行下面
			if votes > len(rf.peers)/2 && rf.state == Candidate {
				rf.becomeLeader()
				go rf.StartAppendEntries(true) //当选为leader后立即发送心跳
			}
		}(i)
	}
}

// 成为领导者后，要更新nextIndex和matchIndex
func (rf *Raft) becomeLeader() {
	rf.state = Leader
	DPrintf("%v: [%d] got enough votes, and now is the leader(currentTerm=%d, state=%v)!starting to append heartbeat...\n", rf.SayMeL(), rf.me, rf.currentTerm, rf.state)
	lastIndex := rf.Log.LastLogIndex // 日志索引从0开始，因此最后一个索引是长度减一

	// 初始化nextIndex
	//rf.nextIndex = make([]int, len(rf.peers))
	//初始化的话，就是还没有日志条目，next都是1，都要从第一个位置开始
	for i := range rf.nextIndex {
		rf.nextIndex[i] = lastIndex + 1 // 下一个要发送的日志条目是最后一个索引加一
	}

	// 初始化matchIndex
	//rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.matchIndex {
		rf.matchIndex[i] = 0 // 开始时没有任何日志条目被确认复制
	}

}

// 发生在某个follower成为了candidate，要进行投票，它的选举时间超时了
func (rf *Raft) resetElectionTimer() {
	rf.lastElection = time.Now()
	rf.electionTimeout = rf.getElectionTime()
	DPrintf("%d has refreshed the electionTimeout at term %d to a random value %d...\n", rf.me, rf.currentTerm, rf.electionTimeout/1000000)
}

// 产生一个随机超时时间
func (rf *Raft) getElectionTime() time.Duration {
	return time.Millisecond * time.Duration(350+rand.Intn(200))
}

func (rf *Raft) HandleHeartbeatRPC(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock() // 加接收心跳方的锁
	defer rf.mu.Unlock()
	reply.FollowerTerm = rf.currentTerm
	reply.Success = true
	// 旧任期的leader抛弃掉
	if args.Term < rf.currentTerm {
		DPrintf("%v: Rejecting heartbeat from leader %d because its term %d is less than my term %d.\n", rf.SayMeL(), args.LeaderId, args.Term, rf.currentTerm)
		reply.Success = false
		return
	}
	//DPrintf(200, "I am %d and the dead state is %d with term %d", rf.me)
	DPrintf("%v: I am now receiving heartbeat from leader %d and dead state is %d with term is %d\n", rf.SayMeL(), args.LeaderId, rf.dead, args.Term)
	rf.resetElectionTimer()
	// 需要转变自己的身份为Follower,有leader产生了
	rf.state = Follower
	// 承认来者是个合法的新leader，则任期一定大于自己，此时需要设置votedFor为-1以及
	if args.Term > rf.currentTerm {
		rf.votedFor = -1
		rf.currentTerm = args.Term
		reply.FollowerTerm = rf.currentTerm
		//rf.persist()
	}

	//lab2B所做修改，对于commitIndex日志的提交

	rf.persist()

	// 重置自身的选举定时器，这样自己就不会重新发出选举需求（因为它在ticker函数中被阻塞住了）
}
