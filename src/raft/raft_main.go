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
	"6.824/labgob"
	"6.824/labrpc"
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
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

	// Persistent state
	currentTerm int     // 节点上的最新任期，初始化为0，单调递增
	votedFor    int     // 当前任期投票所给的候选人id，null if none
	log         []Entry // 日志条目列表，first index is 1

	// Volatile state on all servers
	commitIndex int // 最新的已被提交的日志索引号，初始为0，单调递增
	lastApplied int // 最新的已被应用到状态机的索引号，初始为0，单调递增

	// Volatile state on leaders
	new        bool  // 是否新上任的leader，新上任的leader在crush之前必须至少提交一条日志，可以提交一个空命令携带当前的term，这样不会出现 raft 论文 图8的情况
	nextIndex  []int // 保存需要发送给每个节点的下一条日志条目的索引号，初始化为leader的最后一条日志索引 + 1
	matchIndex []int // 保存所有节点已知的已被复制的最高日志索引号

	state             int // 节点身份
	voteCount         int // 获得的投票数
	chHeartBeat       chan struct{} // 收到心跳信号通知
	chVoteGrant       chan struct{} // 收到投票同意通知
	chGetMajorityVote chan struct{} // 获得大多数投票通知
	chCommit          chan struct{} // 可以提交 通知
	chApply           chan ApplyMsg // 放置apply的消息管道
}

// 日志条目
type Entry struct {
	LogIndex   int `json:"log_index"`
	LogTerm    int `json:"log_term"`
	LogCommand interface{} `json:"log_command"`
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isLeader := rf.state == StateLeader
	return term, isLeader
}

func (rf *Raft) GetPersistSize() int {
	return rf.persister.RaftStateSize()
}

func (rf *Raft) GetLastIndex() int {
	return rf.log[len(rf.log)-1].LogIndex
}

func (rf *Raft) GetBaseIndex() int {
	return rf.log[0].LogIndex
}

func (rf *Raft) GetLastTerm() int {
	return rf.log[len(rf.log)-1].LogTerm
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() { // 在修改了需要存储在非易失性存储上的状态后 需要立即调用
	// Your code here (2C).
	rf.persister.SaveRaftState(rf.getPersistData())
}

func (rf *Raft) getPersistData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	_ = e.Encode(rf.currentTerm)
	_ = e.Encode(rf.votedFor)
	_ = e.Encode(rf.log)
	return w.Bytes()
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	//Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	var votedFor int
	var log []Entry
	if d.Decode(&term) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		_, _ = DPrintf("readPersist error")
	} else {
		rf.currentTerm = term
		rf.votedFor = votedFor
		rf.log = log
		_, _ = DPrintf("readPersist: rf.currentTerm: %d, rf.votedFor: %d, rf.log: %v", term, votedFor, log)
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
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := rf.currentTerm
	isLeader := rf.state == StateLeader
	//if isLeader && !rf.new {
	if isLeader {
		index = rf.GetLastIndex() + 1
		_, _ = DPrintf("leader %d get: %v =============", rf.me, Entry{LogTerm: term, LogIndex: index, LogCommand: command})
		rf.log = append(rf.log, Entry{LogTerm: term, LogIndex: index, LogCommand: command}) // 更新新的log到易失性的存储中
		rf.persist()                                                                        // 保存到非易失性的存储中
	}

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
		_, _ = DPrintf("node: %d, state: %d, log entry: %v", rf.me, rf.state, rf.log)
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
			case <-time.After(time.Duration(rand.Intn(300)+300) * time.Millisecond):
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
			rf.persist()
			rf.mu.Unlock()
			go rf.broadCastRequestVote()
			select {
			case <-time.After(time.Duration(rand.Intn(300)+300) * time.Millisecond):
				rf.mu.Lock()
				_, _ = DPrintf("candidate %d this term fail to win, init next term", rf.me)
				rf.mu.Unlock()
			case <-rf.chHeartBeat:
				rf.mu.Lock()
				rf.state = StateFollower
				_, _ = DPrintf("candidate %d get heart beat, return to follower", rf.me)
				rf.mu.Unlock()
			case <-rf.chGetMajorityVote:
				rf.mu.Lock()
				rf.state = StateLeader
				_, _ = DPrintf("candidate %d get most votes", rf.me)
				rf.nextIndex = make([]int, len(rf.peers))
				rf.matchIndex = make([]int, len(rf.peers))
				//rf.log = append(rf.log, Entry{LogIndex: rf.GetLastIndex() + 1, LogTerm: rf.currentTerm})
				for peer := range rf.peers {
					rf.nextIndex[peer] = rf.GetLastIndex() + 1
					rf.matchIndex[peer] = 0
				}
				//rf.new = true
				go rf.checkCommit()
				rf.mu.Unlock()
			}
		case StateLeader:
			// 领导者：重复间隔发送心跳信道到其他节点(不产生分区的情况下，只可能选出一个领导)
			_, _ = DPrintf("leader %d send heart beat normally", rf.me)
			rf.broadCastAppendEntries()
			time.Sleep(time.Duration(30) * time.Millisecond)
		}
	}
}

func (rf *Raft) updateEntries() {
	for rf.killed() == false {
		select {
		case <-rf.chCommit:
			var msgs []ApplyMsg
			rf.mu.Lock()
			commitIndex := rf.commitIndex
			baseIndex := rf.GetBaseIndex()
			for i := rf.lastApplied + 1; i <= commitIndex; i++ {
				_, _ = DPrintf("node %d, i: %d, baseIndex: %d, commitIndex: %d", rf.me, i, baseIndex, commitIndex)
				msgs = append(msgs, ApplyMsg{CommandIndex: i, CommandValid: true, Command: rf.log[i-baseIndex].LogCommand})
				rf.lastApplied = i
			}
			rf.mu.Unlock()
			// 这里如果在上面的for循环中进行msg放入chApply,会出现活锁
			for _, msg := range msgs {
				rf.chApply <- msg
				_, _ = DPrintf("node %d send apply msg %v", rf.me, msg)
			}
		default:
			time.Sleep(time.Millisecond * 15)
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
	_, _ = DPrintf("node: %d init", rf.me)
	// Your initialization code here (2A, 2B, 2C).
	rf.state = StateFollower
	rf.votedFor = -1
	rf.log = append(rf.log, Entry{LogTerm: 0})
	rf.chHeartBeat = make(chan struct{}, 10)
	rf.chVoteGrant = make(chan struct{}, 10)
	rf.chGetMajorityVote = make(chan struct{}, 10)
	rf.chCommit = make(chan struct{}, 10)
	rf.chApply = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.readSnapshot(persister.ReadSnapshot())

	// start ticker goroutine to start elections
	go rf.ticker()

	// start goroutine to update commit index and apply index
	go rf.updateEntries()

	return rf
}
