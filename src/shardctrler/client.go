package shardctrler

//
// Shardctrler clerk.
//

import (
	"6.824/labrpc"
	"crypto/rand"
	"math/big"
	"sync"
	"time"
)

type Clerk struct {
	servers  []*labrpc.ClientEnd
	ckid     int64
	reqid    int
	leaderid int
	mu       sync.Mutex
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.ckid = nrand()
	ck.reqid = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := QueryArgs{}
	args.CkId = ck.ckid
	ck.mu.Lock()
	args.ReqId = ck.reqid
	ck.reqid++
	ck.mu.Unlock()
	args.Num = num
	leaderId := ck.leaderid

	for {
		// try each known server.
		srv := ck.servers[leaderId]
		var reply QueryReply
		ok := srv.Call("ShardCtrler.Query", args, &reply)
		if ok && reply.Err != WrongLeader {
			ck.leaderid = leaderId
			return reply.Config
		}
		leaderId = (leaderId + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := JoinArgs{}
	args.CkId = ck.ckid
	ck.mu.Lock()
	args.ReqId = ck.reqid
	ck.reqid++
	ck.mu.Unlock()
	args.Servers = servers
	leaderId := ck.leaderid

	for {
		// try each known server.
		var reply JoinReply
		ok := ck.servers[leaderId].Call("ShardCtrler.Join", args, &reply)
		if ok && reply.Err != WrongLeader {
			ck.leaderid = leaderId
			return
		}
		leaderId = (leaderId + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := LeaveArgs{}
	args.CkId = ck.ckid
	ck.mu.Lock()
	args.ReqId = ck.reqid
	ck.reqid++
	ck.mu.Unlock()
	args.GIDs = gids
	leaderId := ck.leaderid

	for {
		// try each known server.
		srv := ck.servers[leaderId]
		var reply LeaveReply
		ok := srv.Call("ShardCtrler.Leave", args, &reply)
		if ok && reply.Err != WrongLeader {
			ck.leaderid = leaderId
			return
		}
		leaderId = (leaderId + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := MoveArgs{}
	args.CkId = ck.ckid
	ck.mu.Lock()
	args.ReqId = ck.reqid
	ck.reqid++
	ck.mu.Unlock()
	args.Shard = shard
	args.GID = gid
	leaderId := ck.leaderid

	for {
		// try each known server.
		srv := ck.servers[leaderId]
		var reply MoveReply
		ok := srv.Call("ShardCtrler.Move", args, &reply)
		if ok && reply.Err != WrongLeader {
			ck.leaderid = leaderId
			return
		}
		leaderId = (leaderId + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}
