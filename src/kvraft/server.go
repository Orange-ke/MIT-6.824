package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const (
	Debug = false

	timeout = 1000 * time.Millisecond
)

func (kv *KVServer) DPrintf(format string, a ...interface{}) (n int, err error) {
	_, isleader := kv.rf.GetState()
	if Debug && isleader {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type  string // put / append / get
	Key   string
	Value string
	CkId  int64 // ckid
	ReqId int   // reqid
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	db     map[string]string // 内存中存储的kv，相当于状态机
	ack    map[int64]int     // 判断req是否已被大多数节点复制
	result map[int]chan Op   // 返回可以apply的msg或需要snapshot
}

func (kv *KVServer) Get(args GetArgs, reply *GetReply) {

	_, _ = kv.DPrintf("get args: %v, Get", args)
	// Your code here.
	command := Op{
		Type:  "Get",
		Key:   args.Key,
		CkId:  args.CkId,
		ReqId: args.ReqId,
	}
	err := kv.AppendLog(command)
	if err != ErrWrongLeader {
		_, _ = kv.DPrintf("ckid: %v, reqid: %v, err: %v", args.CkId, args.ReqId, err)
	}
	if err != OK {
		reply.Err = err
	} else {
		reply.Err = OK
		kv.mu.Lock()
		reply.Value = kv.db[args.Key]
		_, _ = kv.DPrintf("ckid: %v, reqid: %v, key: %v, value: %v", args.CkId, args.ReqId, args.Key, reply.Value)
		kv.ack[args.CkId] = args.ReqId
		kv.mu.Unlock()
	}
}

func (kv *KVServer) PutAppend(args PutAppendArgs, reply *PutAppendReply) {
	if _, isLeader := kv.rf.GetState(); isLeader {
		_, _ = kv.DPrintf("get args: %v, PutAppend", args)
	}
	// Your code here.
	command := Op{
		Type:  args.Op,
		Key:   args.Key,
		Value: args.Value,
		CkId:  args.CkId,
		ReqId: args.ReqId,
	}
	err := kv.AppendLog(command)
	if err != ErrWrongLeader {
		_, _ = kv.DPrintf("ckid: %v, reqid: %v, err: %v", args.CkId, args.ReqId, err)
	}
	reply.Err = err
}

// AppendLog 将请求写入log
func (kv *KVServer) AppendLog(op Op) Err {
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		return ErrWrongLeader
	}
	kv.mu.Lock()
	ch, ok := kv.result[index]
	if !ok {
		ch = make(chan Op, 1)
		kv.result[index] = ch
	}
	kv.mu.Unlock()
	select {
	case command := <-ch:
		if command == op {
			return OK
		}
		return ErrRetry
	case <-time.After(timeout):
		return ErrTimeOut
	}
}

// Apply 应用到状态机 map[string]string
func (kv *KVServer) Apply(op Op) {
	switch op.Type {
	case "Put":
		kv.db[op.Key] = op.Value
		_, _ = kv.DPrintf("op: %v, db state: %v", op, kv.db)
	case "Append":
		kv.db[op.Key] += op.Value
		_, _ = kv.DPrintf("op: %v, db state: %v", op, kv.db)
	}
	kv.ack[op.CkId] = op.ReqId // 记录回复进度
}

// isDup
func (kv *KVServer) isDup(ckid int64, reqid int) bool {
	v, ok := kv.ack[ckid]
	if ok {
		return v < reqid
	}
	return true
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg, 100)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.db = make(map[string]string)
	kv.ack = make(map[int64]int)
	kv.result = make(map[int]chan Op)

	// 处理kv 应用 或 kv 快照
	go kv.checkApply()
	return kv
}

func (kv *KVServer) checkApply() {
	for {
		msg := <-kv.applyCh
		if msg.SnapshotValid { // 需要应用快照
			var lastIncludedIndex int
			var lastIncludedTerm int
			b := bytes.NewBuffer(msg.Snapshot)
			d := labgob.NewDecoder(b)

			_ = d.Decode(&lastIncludedIndex)
			_ = d.Decode(&lastIncludedTerm)

			kv.mu.Lock()
			kv.db = make(map[string]string)
			kv.ack = make(map[int64]int)

			_ = d.Decode(&kv.db)
			_ = d.Decode(&kv.ack)
			kv.mu.Unlock()
		} else { // 应用command
			op := msg.Command.(Op)
			kv.mu.Lock()
			if kv.isDup(op.CkId, op.ReqId) { // 避免重复应用
				_, _ = kv.DPrintf("apply op: %v", op)
				_, _ = kv.DPrintf("now reqid: %v, get reqid: %v", kv.ack[op.CkId], op.ReqId)
				kv.Apply(op)
			}
			ch, ok := kv.result[msg.CommandIndex]
			if ok {
				select {
				case <-kv.result[msg.CommandIndex]: // 取出之前可能遗留的apply信息
				default:
				}
				ch <- op
			} else {
				kv.result[msg.CommandIndex] = make(chan Op, 1)
			}
			if kv.maxraftstate != -1 && kv.rf.GetPersistSize() > kv.maxraftstate {
				w := new(bytes.Buffer)
				e := labgob.NewEncoder(w)
				_ = e.Encode(kv.db)
				_ = e.Encode(kv.ack)
				data := w.Bytes()
				go kv.rf.Snapshot(msg.CommandIndex, data)
			}
			kv.mu.Unlock()
		}
	}
}
