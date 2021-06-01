package raft

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	reply.VoteGranted = false
	if args.Term < rf.currentTerm { // 请求投票携带的任期 小于 接收者的当前任期，则直接返回当前任期通知其过期
		reply.Term = rf.currentTerm
		_, _ = DPrintf("node: %d, state: %d, get vote request with term < self term", rf.me, rf.state)
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = StateFollower
		rf.votedFor = -1
		_, _ = DPrintf("node: %d, state: %d, get vote request with term > self term, change to follower", rf.me, rf.state)
	}
	reply.Term = rf.currentTerm
	// 判断是否候选人的日志 新于或等于 接收人的日志
	var isLatest bool
	latestTerm := rf.GetLastTerm()
	latestIndex := rf.GetLastIndex()
	if args.LastLogTerm > latestTerm {
		isLatest = true
	}

	if args.LastLogTerm == latestTerm && args.LastLogIndex >= latestIndex {
		isLatest = true
	}

	if rf.votedFor == -1 {
		_, _ = DPrintf("node: %d, state: %d, hasn't vote for any node", rf.me, rf.state)
	}

	if isLatest {
		_, _ = DPrintf("node: %d, state: %d, candidate %d log is at least as new as me", rf.me, rf.state, args.CandidateId)
	} else {
		_, _ = DPrintf("node: %d, state: %d, candidate %d log is not as new as me", rf.me, rf.state, args.CandidateId)
	}

	// 当voteFor为null或candidateId，并且isLatest为true，则返回进行投票
	if (rf.votedFor == -1 || args.CandidateId == rf.votedFor) && isLatest {
		rf.chVoteGrant <- struct{}{}
		rf.state = StateFollower
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		_, _ = DPrintf("node: %d, state: %d, grant vote to candidate", rf.me, rf.state)
	}
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
			_, _ = DPrintf("candidate %d: term change: %d <-> %d", rf.me, rf.currentTerm, args.Term)
			return ok
		}
		// 发起投票接受到的结果中携带的任期大于自己的任期时：
		if reply.Term > rf.currentTerm {
			_, _ = DPrintf("candidate: %d vote request get reply term > self term, change to follower", rf.me)
			rf.currentTerm = reply.Term
			rf.state = StateFollower
			rf.votedFor = -1
			rf.persist()
		}
		// 候选人获取的选票
		if reply.VoteGranted {
			rf.voteCount++
			_, _ = DPrintf("candidate: %d vote request get granted, count: %d", rf.me, rf.voteCount)
			if rf.state == StateCandidate && rf.voteCount > len(rf.peers)/2 {
				_, _ = DPrintf("candidate: %d vote request get most votes", rf.me)
				rf.state = StateFollower
				rf.chGetMajorityVote <- struct{}{}
			}
		}
	}
	return ok
}

// 发送选举消息到所有其他节点
func (rf *Raft) broadCastRequestVote() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogTerm:  rf.GetLastTerm(),
		LastLogIndex: rf.GetLastIndex(),
	}

	for index := range rf.peers {
		if index != rf.me && rf.state == StateCandidate {
			_, _ = DPrintf("candidate %d send vote request to node %d", rf.me, index)
			go func(index int) {
				var reply RequestVoteReply
				rf.sendRequestVote(index, args, &reply)
			}(index)
		}
	}
}
