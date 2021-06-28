package kvraft

import (
	"6.824/labrpc"
	"crypto/rand"
	"log"
	"math/big"
	"sync"
	"time"
)

const ChangeLeaderInterval = 200 * time.Millisecond

type Clerk struct {
	servers []*labrpc.ClientEnd
	ckid int64
	reqid int
	leaderid int
	mu sync.Mutex
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

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	args := GetArgs{Key: key, CkId: ck.ckid}
	ck.mu.Lock()
	args.ReqId = ck.reqid
	ck.reqid++
	ck.mu.Unlock()
	serverIdx := ck.leaderid
	for {
		reply := GetReply{}
		ok := ck.servers[serverIdx].Call("KVServer.Get", args, &reply)
		if !ok {
			time.Sleep(ChangeLeaderInterval)
			serverIdx = (serverIdx + 1) % len(ck.servers)
			continue
		}
		switch reply.Err {
		case OK:
			ck.leaderid = serverIdx
			return reply.Value
		case ErrWrongLeader:
			serverIdx = (serverIdx + 1) % len(ck.servers)
			continue
		case ErrNoKey:
			ck.leaderid = serverIdx
			return ""
		case ErrTimeOut:
			continue
		default:
			time.Sleep(ChangeLeaderInterval)
			serverIdx = (serverIdx + 1) % len(ck.servers)
			continue
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{Key: key, Value: value, Op: op, CkId: ck.ckid}
	ck.mu.Lock()
	args.ReqId = ck.reqid
	ck.reqid++
	ck.mu.Unlock()
	serverIdx := ck.leaderid
	for {
		reply := PutAppendReply{}
		ok := ck.servers[serverIdx].Call("KVServer.PutAppend", args, &reply)
		if !ok {
			time.Sleep(ChangeLeaderInterval)
			serverIdx = (serverIdx + 1) % len(ck.servers)
			continue
		}
		switch reply.Err {
		case OK:
			ck.leaderid = serverIdx
			return
		case ErrNoKey:
			log.Fatal("client putappend get err nokey")
		case ErrWrongLeader:
			serverIdx = (serverIdx + 1) % len(ck.servers)
			continue
		case ErrTimeOut:
			continue
		default:
			time.Sleep(ChangeLeaderInterval)
			serverIdx = (serverIdx + 1) % len(ck.servers)
			continue
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
