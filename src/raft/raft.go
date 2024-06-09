package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"fmt"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}
type RaftState int

const (
	Follower RaftState = iota + 1
	Candidate
	Leader
)

const heartbeatTimeout = 150 * time.Millisecond //心跳超时时间150ms，满足要求不能1s内超过10次
const tickerInterval = 80 * time.Millisecond

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state            RaftState     //节点的状态角色
	currentTerm      int           //现在的任期号
	votedFor         int           //投票给谁了
	heartbeatTimeout time.Duration // 心跳定时器
	electionTimeout  time.Duration //选举计时器
	lastElection     time.Time     // 上一次的选举时间，用于配合since方法计算当前的选举时间是否超时
	lastHeartbeat    time.Time     // 上一次的心跳时间，用于配合since方法计算当前的心跳时间是否超时
	nextElectionTime time.Time     //下一个超时的时间戳
	// 正常情况下commitIndex与lastApplied应该是一样的，但是如果有一个新的提交，并且还未应用的话last应该要更小些
	commitIndex int // 状态机中已知的被提交的日志条目的索引值(初始化为0，持续递增）
	lastApplied int // 最后一个被追加到状态机日志的索引值
	Log         *Log
	// leader可见的变量
	// nextIndex指的是下一个的appendEntries要从哪里开始
	// matchIndex指的是已知的某follower的log与leader的log最大匹配到第几个Index,已经apply
	nextIndex  []int // 对于每一个server，需要发送给他下一个日志条目的索引值（初始化为leader日志index+1,那么范围就对标len） 记录每一个服务器日志更新的进度
	matchIndex []int // 对于每一个server，已经复制给该server的最后日志条目下标

	applyCh chan ApplyMsg
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int // 领导者认为的follower上次日志更新到的最后一个索引位置
	PrevLogTerm  int
	Logs         []Entry
	LeaderCommit int // Leader已经提交的最高的日志的索引
}

type AppendEntriesReply struct {
	Success       bool //	如果follower与Args中的PreLogIndex/PreLogTerm都匹配才会接过去新的日志（追加），不匹配直接返回false
	FollowerTerm  int
	ConflictIndex int // 冲突的任期内出现最早的日志索引位置
	ConflictTerm  int //  这个说明prevLogIndex对应的日志的那个任期term与PrevLogTerm不一致，这是他在follower中的term
}

// 这个方法被测试来调用验证任期和领导者的
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = (rf.state == Leader)
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	PeerTerm    int  //这个节点的term
	VoteGranted bool //true表示支持他
}

// 任何服务器收到了这个请求投票的rpc后进行的逻辑判断
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 检查请求的term是否比当前节点的term小
	if args.Term < rf.currentTerm {
		// 如果小于当前的term，拒绝投票
		reply.PeerTerm = rf.currentTerm
		reply.VoteGranted = false
		rf.persist()
		return
	}

	// 如果请求的term大于当前的term，当前节点应该变成follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1 // 重置已投票节点
		rf.state = Follower
		rf.persist()
	}

	// 两个条件决定是否投票给请求节点：
	// 1. 当前节点在这个term还没投过票或者已经投给了请求的候选人
	// 2. 请求的候选人的日志至少要和当前节点的日志一样新
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.isLogUpToDate(args.LastLogIndex, args.LastLogTerm) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId // 给这个候选人投票
		reply.PeerTerm = rf.currentTerm
		rf.state = Follower
		rf.resetElectionTimer() //这个节点应当重置选举超时时间
		DPrintf("%v: 投出同意票给节点 %d\n", rf.SayMeL(), args.CandidateId)
		rf.persist()
	} else {
		// 不满足以上条件，拒绝投票
		// 一个candidate在拉票时，如果另一个也变成candidate跟他拉票，明显是拒绝的
		reply.VoteGranted = false
		reply.PeerTerm = rf.currentTerm

		DPrintf("%v: 投出反对票给节点 %d\n", rf.SayMeL(), args.CandidateId)

	}
	return
}

func (rf *Raft) SayMeL() string {

	//return fmt.Sprintf("[Server %v as %v at term %v with votedFor %d, FirstLogIndex %d, LastLogIndex %d,  commitIndex %d, and lastApplied %d]： + \n",
	//	rf.me, rf.state, rf.currentTerm, rf.votedFor, rf.Log.FirstLogIndex, rf.Log.LastLogIndex, rf.commitIndex, rf.lastApplied)
	return fmt.Sprintf("[Server %v]: ", rf.me)
	//return "success"
}

// Helper function to check if the candidate's log is at least as up-to-date as receiver's log
func (rf *Raft) isLogUpToDate(candidateLastIdx, candidateLastTerm int) bool {
	lastIndex := rf.Log.LastLogIndex
	lastTerm := rf.getLastEntryTerm()
	return candidateLastTerm >= lastTerm && candidateLastIdx >= lastIndex
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := rf.Log.LastLogIndex + 1 //LastLogIndex就指向了最后一个日志的索引位置
	term := rf.currentTerm
	isLeader := (rf.state == Leader)
	if rf.killed() {
		return -1, -1, false
	}

	// Your code here (2B).
	if isLeader {
		entry := Entry{
			Term:    term,
			Command: command,
		}
		rf.Log.Entries = append(rf.Log.Entries, entry)
		rf.Log.LastLogIndex = index
		rf.nextIndex[rf.me]++
		rf.matchIndex[rf.me] = rf.nextIndex[rf.me] - 1
		DPrintf("Leader %d added a new log entry at index %d with term %d, command: %v", rf.me, index, term, command)
		for peer := range rf.peers {
			if peer == rf.me {
				continue
			}
			DPrintf("Leader %d ------->  follower %d", rf.me, peer)
			go rf.AppendEntries(peer, false, rf.Log.Entries)
		}

	}
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

// killed来检查当前实例是否已经dead了，也就是使用过Kill
func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()
		switch state {
		case Follower:
			fallthrough
		case Candidate:
			if rf.pastElectionTimeOut() {
				//这里存在俩种情况一个是，一个follower没有在选举超时的时间内收到心跳，所以变成candidate进行一轮选举
				//还有一种是当前candidate没有获得选举的胜利，重新又开启了一轮
				rf.StartElection()
			}
		case Leader:
			isHeartBeat := false
			if rf.pastHeartBeatTimeOut() {
				isHeartBeat = true
				rf.resetHeartBeatTimeOut()
			}
			rf.StartAppendEntries(isHeartBeat)
		}

		time.Sleep(tickerInterval)
	}
}

// 广播心跳
func (rf *Raft) StartAppendEntries(heart bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.resetElectionTimer()
	if rf.state != Leader {
		return
	}
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.AppendEntries(i, true, []Entry{})
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//

// peers是网络标识符数组,me就是当前这个peer对应的下标
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.resetElectionTimer() //初始化上次选举超时时间和上次接收到leader/candidate RPC的时间
	rf.resetHeartBeatTimeOut()
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.heartbeatTimeout = heartbeatTimeout
	rf.Log = NewLog()
	rf.applyCh = applyCh
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func (rf *Raft) AppendEntries(targetServerId int, heart bool, entries []Entry) {

	if heart {
		rf.mu.Lock()
		// 检查是否是领导者
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}
		args := AppendEntriesArgs{
			Term:     rf.currentTerm,
			LeaderId: rf.me,
		}
		rf.mu.Unlock()

		DPrintf("%v: %d is a leader, ready sending heartbeart to follower %d....\n", rf.SayMeL(), rf.me, targetServerId)
		reply := AppendEntriesReply{}
		ok := rf.sendRequestAppendEntries(true, targetServerId, &args, &reply)

		rf.mu.Lock()
		if !ok || rf.state != Leader {
			rf.mu.Unlock()
			return
		}
		DPrintf("%v: 发送完心跳收到对方的Term为%d ,而我的Term为%d \n", rf.SayMeL(), reply.FollowerTerm, rf.currentTerm)
		switch {
		//如果发送心跳的目标方term比当前的大，立即沦落为follower
		case reply.FollowerTerm > rf.currentTerm:
			rf.currentTerm = reply.FollowerTerm
			rf.votedFor = -1
			rf.state = Follower
			rf.persist()
		case reply.FollowerTerm < rf.currentTerm:
			// 旧的RPC响应，忽略
		default:
			// 如果是当前任期的有效响应，且reply.Success为false时需要特别处理
			if !reply.Success {
				// 可以添加处理失败响应的逻辑
			}
		}
		rf.mu.Unlock()
	} else {
		//发送最新的log
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}

		prevLogIndex := rf.nextIndex[targetServerId] - 1
		prevLogTerm := -1
		if prevLogIndex >= 0 {
			prevLogTerm = rf.Log.Entries[prevLogIndex].Term
		}
		entries := append([]Entry{}, rf.Log.Entries[rf.nextIndex[targetServerId]:]...)
		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			LeaderCommit: rf.commitIndex,
			Logs:         entries,
		}
		rf.mu.Unlock() // Release lock before I/O operation

		var reply AppendEntriesReply
		if rf.sendRequestAppendEntries(false, targetServerId, &args, &reply) {
			rf.mu.Lock() // Re-acquire lock to handle the reply
			DPrintf("Leader %d received a reply from %d for AppendEntries: Success=%v", rf.me, targetServerId, reply.Success)
			if rf.state == Leader { // Double-check the state
				rf.handleAppendEntriesReply(targetServerId, &args, &reply)
			}
			rf.mu.Unlock()
		}

	}
}
func (rf *Raft) sendRequestAppendEntries(isHeartbeat bool, server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	var ok bool
	if isHeartbeat {
		ok = rf.peers[server].Call("Raft.HandleHeartbeatRPC", args, reply)
	} else {
		ok = rf.peers[server].Call("Raft.HandleAppendEntriesRPC", args, reply)
	}
	return ok
}
