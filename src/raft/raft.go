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
	"math/rand"

	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

// 节点状态
const (
	StateFollower = iota
	StateCandidate
	StateLeader
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
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

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state
	currentTerm int // 节点上的最新任期，初始化为0，单调递增
	votedFor int // 当前任期投票所给的候选人id，null if none
	log []Entry // 日志条目列表，first index is 1

	// Volatile state on all servers
	commitIndex int // 最新的已被提交的日志索引号，初始为0，单调递增
	lastApplied int // 最新的已被应用到状态机的索引号，初始为0，单调递增

	// Volatile state on leaders
	nextIndex []int // 保存需要发送给每个节点的下一条日志条目的索引号，初始化为leader的最后一条日志索引 + 1
	matchIndex []int // 保存所有节点已知的已被复制的最高日志索引号

	//
	state int
	voteCount int
	chHeartBeat chan struct{}
	chVoteGrant chan struct{}
	chGetMajorityVote chan struct{}
}

// 日志条目
type Entry struct {
	LogIndex int
	LogTerm int
	LogCommand interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	term := rf.currentTerm
	isLeader := rf.state == StateLeader
	rf.mu.Unlock()
	return term, isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
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


//
// restore previously persisted state.
//
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


//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
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


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term int // 领导者的任期号
	LeaderId int // 领导者在peers中的下标
	PrevLogIndex int // 本次新增日志的前一个位置日志的索引值
	PreLogTerm int // 本次新增日志的前一个位置日志的任期号
	Entries []Entry // 追加到follower上的日志条目
	LeaderCommit int // 领导者已提交的日志条目的索引值
}

type AppendEntriesReply struct {
	Term int // 当前任期
	Success bool // 当跟随者中的日志情况于leader保持一致时为true，多退少补
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.VoteGranted = false
	if args.Term < rf.currentTerm { // 请求投票携带的任期 小于 接收者的当前任期，则直接返回当前任期通知其过期
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = StateFollower
		rf.votedFor = -1
	}
	reply.Term = rf.currentTerm
	// 判断是否候选人的日志 新于或等于 接收人的日志
	var isLatest bool
	latestTerm := rf.log[len(rf.log) - 1].LogTerm
	latestIndex := rf.log[len(rf.log) - 1].LogIndex
	if args.LastLogTerm > latestTerm  {
		isLatest = true
	}
	if args.LastLogTerm == latestTerm && args.LastLogIndex >= latestIndex {
		isLatest = true
	}
	// 当voteFor为null或candidateId，并且isLatest为true，则返回进行投票
	if (rf.votedFor == -1 || args.CandidateId == rf.votedFor) && isLatest {
		rf.chVoteGrant <- struct{}{}
		rf.state = StateFollower
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
	}
}

// AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Success = false
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		return
	}
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.state = StateFollower
		rf.votedFor = -1
	}
	reply.Term = rf.currentTerm
	rf.chHeartBeat <- struct{}{}
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply) // 利用反射调用Raft.RequestVote
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		if rf.state != StateCandidate {
			return ok
		}
		if rf.currentTerm != args.Term { // 任期变动
			return ok
		}
		// 发起投票接受到的结果中携带的任期大于自己的任期时：
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = StateFollower
			rf.votedFor = -1
		}
		// 候选人获取的选票
		if reply.VoteGranted {
			rf.voteCount++
			if rf.state == StateCandidate && rf.voteCount > len(rf.peers) / 2 {
				rf.state = StateFollower
				rf.chGetMajorityVote <- struct{}{}
			}
		}
	}
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		if rf.state != StateLeader {
			return ok
		}
		if rf.currentTerm != args.Term { // 任期变动
			return ok
		}
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = StateFollower
			rf.votedFor = -1
			return ok
		}
		// 根据返回的 success 信息，判断 log 的复制情况
	}

	return ok
}

// 发送选举消息到所有其他节点
func (rf *Raft) broadCastRequestVote() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	args := RequestVoteArgs{
		Term: rf.currentTerm,
		CandidateId: rf.me,
		LastLogTerm: rf.log[len(rf.log) - 1].LogTerm,
		LastLogIndex: rf.log[len(rf.log) - 1].LogIndex,
	}

	for index := range rf.peers {
		if index != rf.me && rf.state == StateCandidate {
			go func(index int) {
				var reply RequestVoteReply
				rf.sendRequestVote(index, args, &reply)
			}(index)
		}
	}
}

// 发送appendEntries消息到其他所有节点
func (rf *Raft) broadCastAppendEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	args := AppendEntriesArgs{
		Term: rf.currentTerm,
		LeaderId: rf.me,
		//
	}

	for index := range rf.peers {
		if index != rf.me && rf.state == StateLeader {
			go func(index int) {
				var reply AppendEntriesReply
				rf.sendAppendEntries(index, args, &reply)
			}(index)
		}
	}
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()
		// Your code here to check if a leader election should
		// be started and to randomize timeout
		switch state {
		case StateFollower:
			// 跟随者：需要对leader 和 candidate 的 RPC 做出反阔
			// 在超时时间内没有收到leader的心跳信号或者candidate的竞选信号，则将身份变为candidate
			select {
			case <-rf.chHeartBeat:
			case <-rf.chVoteGrant:
			case <- time.After(time.Duration(rand.Intn(500) + 500) * time.Millisecond):
				rf.mu.Lock()
				rf.state = StateCandidate
				rf.mu.Unlock()
			}
		case StateCandidate:
			// 候选人：增加当前任期，给自己投票，重置竞选超时时间，发送竞选请求到其他节点
			// 收到大多数投票，则更新身份为leader
			// 收到更新的领导者的心跳信号，则更新身份为follower
			// 选举超时则开始新的一轮选举
			rf.mu.Lock()
			rf.currentTerm++
			rf.votedFor = rf.me
			rf.voteCount = 1
			rf.mu.Unlock()
			go rf.broadCastRequestVote()
			select {
			case <-time.After(time.Duration(rand.Intn(500) + 500) * time.Millisecond):
			case <-rf.chHeartBeat:
				rf.state = StateFollower
			case <-rf.chGetMajorityVote:
				rf.state = StateLeader
			}
		case StateLeader:
			// 领导者：重复间隔发送心跳信道到其他节点(不产生分区的情况下，只可能选出一个领导)
			rf.broadCastAppendEntries()
			time.Sleep(time.Duration(100) * time.Millisecond)
		}
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = StateFollower
	rf.votedFor = -1
	rf.log = append(rf.log, Entry{LogTerm: 0})
	rf.chHeartBeat = make(chan struct{}, 10)
	rf.chVoteGrant = make(chan struct{}, 10)
	rf.chGetMajorityVote = make(chan struct{}, 10)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()


	return rf
}
