package raft

import (
	"6.824/labgob"
	"bytes"
)

// 暂时不考虑分批快照
type InstallSnapShotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapShotReply struct {
	Term int
}

// sender
func (rf *Raft) sendInstallSnapShot(server int, args InstallSnapShotArgs, reply *InstallSnapShotReply) bool {
	_, _ = DPrintf("send snapshot")
	ok := rf.peers[server].Call("Raft.InstallSnapShot", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		if rf.state != StateLeader || rf.currentTerm != args.Term {
			return ok
		}
		if reply.Term > rf.currentTerm {
			rf.state = StateFollower
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.persist()
			return ok
		}
		_, _ = DPrintf("leader %d send snapshot to node %d with success reply", rf.me, server)
		rf.nextIndex[server] = args.LastIncludedIndex + 1
		rf.matchIndex[server] = rf.nextIndex[server] - 1
	}
	return ok
}

// receiver
func (rf *Raft) InstallSnapShot(args InstallSnapShotArgs, reply *InstallSnapShotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 1. 如果leader发来的install消息中的term小于该节点的term，则立即返回该节点的term
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	{
		rf.chHeartBeat <- struct{}{}
		rf.state = StateFollower
		rf.votedFor = -1
		rf.currentTerm = args.Term
	}
	// 2.先持久化保存snapshot和对应的raft状态
	rf.persister.SaveStateAndSnapshot(rf.getPersistData(), args.Data)
	// 3.如果该节点的log中包含lastIncludedIndex 和 lastIncludedTerm对应的log，则保留匹配项之后的日志
	// 否则，丢弃整个log
	rf.log = intercept(args.LastIncludedIndex, args.LastIncludedTerm, rf.log)

	rf.lastApplied = args.LastIncludedIndex
	rf.commitIndex = args.LastIncludedIndex
	_, _ = DPrintf("node %d get snap shot, log change to %v, lastApplied: %d", rf.me, rf.log, args.LastIncludedIndex)
	rf.persist()
	// 4.使用当前的快照信息重置 状态机内容
	applyMsg := ApplyMsg{SnapshotValid: true, Snapshot: args.Data, SnapshotIndex: args.LastIncludedIndex, SnapshotTerm: args.LastIncludedTerm}
	go func() {
		rf.chApply <- applyMsg
		_, _ = DPrintf("node %d get snapshot and send to chApply: %v", rf.me, applyMsg)
	}()
}

// 根据lastIncludedIndex 和 lastIncludedTerm 切割log
func intercept(lastIncludedIndex, lastIncludedTerm int, log []Entry) []Entry {
	var newEntries []Entry
	newEntries = append(newEntries, Entry{LogIndex: lastIncludedIndex, LogTerm: lastIncludedTerm})
	for i := len(log) - 1; i >= 0; i-- {
		if log[i].LogIndex == lastIncludedIndex && log[i].LogTerm == lastIncludedTerm {
			newEntries = append(newEntries, log[i+1:]...)
			break
		}
	}
	return newEntries
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	lastIndex := rf.GetLastIndex()
	if lastIndex > lastIncludedIndex { // 已有比snapshot更新的的状态，则拒绝进行快照
		return false
	}

	rf.log = intercept(lastIncludedIndex, lastIncludedTerm, rf.log)
	_, _ = DPrintf("service send snapshot with lastIncludedTerm %d, lastIncludedIndex %d to node %d, node log change to %v",
		lastIncludedTerm, lastIncludedIndex, rf.me, rf.log)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	_ = e.Encode(lastIncludedTerm)
	_ = e.Encode(lastIncludedIndex)
	data := w.Bytes()
	data = append(w.Bytes(), snapshot...)
	rf.persister.SaveStateAndSnapshot(rf.getPersistData(), data)

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	baseIndex := rf.GetBaseIndex()
	if index <= baseIndex || index > rf.commitIndex { // 有可能在 生成快照 前 获取了 leader节点的快照信息
		_, _ = DPrintf("node: %d, index: %d, baseIndex: %d, commitIndex: %d", rf.me, index, baseIndex, rf.commitIndex)
		return
	}
	var newEntries []Entry
	newEntries = append(newEntries, Entry{LogIndex: index, LogTerm: rf.log[index - baseIndex].LogTerm})
	newEntries = append(newEntries, rf.log[index + 1 - baseIndex:]...)
	rf.log = newEntries
	_, _ = DPrintf("service send snapshot with index %d to node %d, node log change to %v", index, rf.me, rf.log)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	_ = e.Encode(newEntries[0].LogIndex)
	_ = e.Encode(newEntries[0].LogTerm)
	data := w.Bytes()
	data = append(data, snapshot...)
	rf.persister.SaveStateAndSnapshot(rf.getPersistData(), data)
}

func (rf *Raft) readSnapshot(data []byte) {
	rf.readPersist(rf.persister.ReadRaftState())
	if len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var lastIncludedIndex int
	var lastIncludedTerm int

	_ = d.Decode(&lastIncludedIndex)
	_ = d.Decode(&lastIncludedTerm)

	rf.commitIndex = lastIncludedIndex
	rf.lastApplied = lastIncludedIndex
	_, _ = DPrintf("node %d restart read snapshot: lastIncludedIndex = %d, lastIncludedTerm = %d \n", rf.me, lastIncludedIndex, lastIncludedTerm)
	rf.log = intercept(lastIncludedIndex, lastIncludedTerm, rf.log)

	msg := ApplyMsg{SnapshotValid: true, Snapshot: data, SnapshotIndex: lastIncludedIndex, SnapshotTerm: lastIncludedTerm}

	go func() { // 由于chApply是无缓冲区的channel所以需要 启动一个协程来放入
		rf.chApply <- msg
	}()
}