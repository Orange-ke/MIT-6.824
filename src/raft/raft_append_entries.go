package raft

import "time"

type AppendEntriesArgs struct {
	Term         int     // 领导者的任期号
	LeaderId     int     // 领导者在peers中的下标
	PrevLogIndex int     // 本次新增日志的前一个位置日志的索引值
	PreLogTerm   int     // 本次新增日志的前一个位置日志的任期号
	Entries      []Entry // 追加到follower上的日志条目
	LeaderCommit int     // 领导者已提交的日志条目的索引值
}

type AppendEntriesReply struct {
	Term          int  // 当前任期
	Success       bool // 当跟随者中的日志情况于leader保持一致时为true，多退少补
	ConflictIndex int  // -1代表冲突了
	ConflictTerm  int  // -1代表冲突了
}

// AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	reply.Success = false
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		return
	}
	rf.chHeartBeat <- struct{}{}

	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.state = StateFollower
		rf.votedFor = -1
		_, _ = DPrintf("node: %d, state: %d, get heart beat with term > self term, change to follower", rf.me, rf.state)
	}
	reply.Term = rf.currentTerm
	_, _ = DPrintf("node: %d, state: %d, get leader heart beat", rf.me, rf.state)

	_, _ = DPrintf("node: %d, args.PrevLogIndex: %d - rf.GetLastIndex(): %d", rf.me, args.PrevLogIndex, rf.GetLastIndex())
	if args.PrevLogIndex > rf.GetLastIndex() {
		// 节点在prevLogIndex处不含有日志记录
		reply.ConflictIndex = len(rf.log)
		return
	}
	baseIndex := rf.GetBaseIndex()
	if args.PrevLogIndex >= baseIndex {
		if rf.log[args.PrevLogIndex - baseIndex].LogTerm != args.PreLogTerm { // 节点在prevLogIndex处有对应的日志记录，但是term不一致
			reply.ConflictTerm = rf.log[args.PrevLogIndex - baseIndex].LogTerm
			for i := args.PrevLogIndex - 1; i >= baseIndex; i-- { // 找到节点对应该term不一致的第一个index位置
				if rf.log[i-baseIndex].LogTerm != reply.ConflictTerm {
					reply.ConflictIndex = i + 1
					break
				}
			}
			return
		}
	}

	if args.PrevLogIndex >= baseIndex {
		rf.log = rf.log[:args.PrevLogIndex - baseIndex + 1]
		rf.log = append(rf.log, args.Entries...)
		_, _ = DPrintf("node: %d, state: %d, log length: %d", rf.me, rf.state, len(rf.log))
		reply.Success = true
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = Min(args.LeaderCommit, rf.GetLastIndex())
		rf.chCommit <- struct{}{}
	}
	return
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
			_, _ = DPrintf("leader %d: term change: %d <-> %d", rf.me, rf.currentTerm, args.Term)
			return ok
		}
		if reply.Term > rf.currentTerm {
			_, _ = DPrintf("leader: %d heart beat get reply term > self term, change to follower", rf.me)
			rf.currentTerm = reply.Term
			rf.state = StateFollower
			rf.votedFor = -1
			rf.persist()
			return ok
		}
		_, _ = DPrintf("leader: %d heart beat get reply term: %d, self term %d", rf.me, reply.Term, rf.currentTerm)
		// 根据返回的 success 信息，判断 log 的复制情况
		if reply.Success { // 成功则修改对应节点的日志复制对应的状态
			if len(args.Entries) > 0 {
				rf.nextIndex[server] = args.Entries[len(args.Entries)-1].LogIndex + 1 // 下次需要复制的最新下标
				rf.matchIndex[server] = rf.nextIndex[server] - 1                      // 已知的被复制的最高的index
				_, _ = DPrintf("leader next index: %v, match index: %v", rf.nextIndex, rf.matchIndex)
			}
		} else {
			rf.nextIndex[server] = reply.ConflictIndex // 否则将下标减一重试
			_, _ = DPrintf("leader next index: %v", rf.nextIndex)
		}
		return ok
	}

	return ok
}

// 发送appendEntries消息到其他所有节点
func (rf *Raft) broadCastAppendEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	baseIndex := rf.GetBaseIndex()
	for peer := range rf.peers {
		if peer != rf.me && rf.state == StateLeader {
			_, _ = DPrintf("leader %d send heart beat to node %d", rf.me, peer)
			if rf.nextIndex[peer] > baseIndex { // append entry & heart beat
				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: rf.nextIndex[peer] - 1,
					PreLogTerm:   rf.log[rf.nextIndex[peer]-baseIndex-1].LogTerm,
					Entries:      append([]Entry{}, rf.log[rf.nextIndex[peer]-baseIndex:]...),
					LeaderCommit: rf.commitIndex,
				}
				p := peer
				go func(p int, args AppendEntriesArgs) {
					var reply AppendEntriesReply
					_, _ = DPrintf("leader: send %v to node %d -------", args, p)
					rf.sendAppendEntries(p, args, &reply)
				}(p, args)
			} else { // 说明此时该节点落后 snapshot
				args := InstallSnapShotArgs{
					Term: rf.currentTerm,
					LeaderId: rf.me,
					LastIncludedIndex: rf.log[0].LogIndex,
					LastIncludedTerm: rf.log[0].LogTerm,
					Data: rf.persister.snapshot,
				}
				reply := InstallSnapShotReply{}
				p := peer
				go func(p int, args InstallSnapShotArgs) {
					_, _ = DPrintf("leader %d send snapshot to node %d", rf.me, p)
					rf.sendInstallSnapShot(p, args, &reply)
				}(p, args)
			}
		}
	}
}

func (rf *Raft) checkCommit() {
	for {
		rf.mu.Lock()
		// 发送下一次append entries时，先判断一下leader自己有没有需要提交的log项
		// 判断逻辑为：
		// 1. 从上一次的commit index + 1 到 last index为止，那些log是大多数node已经复制了的
		// 2. 找到已经可以commit的日志则通知进行commit
		// 3. 只要对应的index已经认为可以committed，其之前的index也间接的认为是committed
		// 4. 通过apply，从记录的applied 到 committed index 都会应用对应的command
		newCommitIndex := rf.commitIndex
		lastIndex := rf.GetLastIndex()
		baseIndex := rf.GetBaseIndex()
		for i := rf.commitIndex + 1; i <= lastIndex; i++ {
			count := 1
			for peer := range rf.peers {
				if peer != rf.me && rf.matchIndex[peer] >= i && rf.log[i-baseIndex].LogTerm == rf.currentTerm {
					// rf.log[i - baseIndex].LogTerm == rf.currentTerm 表示leader不会覆盖已有的日志
					count++
				}
			}
			if count > len(rf.peers)/2 {
				newCommitIndex = i
			}
		}
		if newCommitIndex != rf.commitIndex {
			_, _ = DPrintf("leader update commit index to %d", newCommitIndex)
			rf.commitIndex = newCommitIndex
			//if rf.commitIndex == rf.GetLastIndex() {
			//	if rf.new {
			//		rf.new = false
			//	}
			//}
			rf.chCommit <- struct{}{}
		}
		rf.mu.Unlock()
		time.Sleep(time.Millisecond * 10)
	}
}