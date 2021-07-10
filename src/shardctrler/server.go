package shardctrler

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"log"
	"reflect"
	"sort"
	"sync"
	"time"
)

const Debug = false

func (sc *ShardCtrler) DPrintf(format string, a ...interface{}) (n int, err error) {
	_, isLeader := sc.rf.GetState()
	if Debug && isLeader {
		log.Printf(format, a...)
	}
	return
}

func init() {
	labgob.Register(JoinArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})
	labgob.Register(QueryArgs{})
}

const (
	timeout = 2 * time.Second
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	configs  []Config            // 存储config
	ack      map[int64]int       // 记录处理过的重配置请求，防止重复处理
	result   map[int]chan Notify //
	shutdown chan struct{}       // 关闭
}

type Notify struct {
	Op   interface{}
	Args interface{}
}

// 添加新的group
func (sc *ShardCtrler) Join(args JoinArgs, reply *JoinReply) {
	_, _ = sc.DPrintf("JoinArgs op: %v", args)
	// Your code here.
	reply.Err, _ = sc.appendLog(args)
}

// 删除group
func (sc *ShardCtrler) Leave(args LeaveArgs, reply *LeaveReply) {
	_, _ = sc.DPrintf("LeaveArgs op: %v", args)
	// Your code here.
	reply.Err, _ = sc.appendLog(args)
}

// 移动group到新的shard
func (sc *ShardCtrler) Move(args MoveArgs, reply *MoveReply) {
	_, _ = sc.DPrintf("MoveArgs op: %v", args)
	// Your code here.
	reply.Err, _ = sc.appendLog(args)
}

// 获取配置列表
func (sc *ShardCtrler) Query(args QueryArgs, reply *QueryReply) {
	_, _ = sc.DPrintf("QueryArgs op: %v", args)
	// Your code here.
	var config interface{}
	reply.Err, config = sc.appendLog(args)

	sc.mu.Lock()
	defer sc.mu.Unlock()
	if reply.Err == OK {
		reply.Config = config.(Config)
	}
}

func (sc *ShardCtrler) appendLog(op interface{}) (Err, interface{}) {
	_, _ = sc.DPrintf("Get origin args: %v", op)
	index, _, isLeader := sc.rf.Start(op)
	_, _ = sc.DPrintf("index: %v, isleader: %v", index, isLeader)
	if !isLeader {
		return WrongLeader, struct{}{}
	}
	sc.mu.Lock()
	ch, ok := sc.result[index]
	if !ok {
		ch = make(chan Notify, 1)
		sc.result[index] = ch
	}
	sc.mu.Unlock()
	select {
	case result := <-ch:
		_, _ = sc.DPrintf("apply success: %v", result.Op)
		sc.mu.Lock()
		delete(sc.result, index)
		sc.mu.Unlock()
		if !reflect.DeepEqual(result.Op, op) {
			return WrongLeader, struct{}{}
		}
		return OK, result.Args
	case <-time.After(timeout):
		sc.mu.Lock()
		if _, isLeader := sc.rf.GetState(); !isLeader { // 已经不是leader则丢弃
			delete(sc.result, index)
		}
		sc.mu.Unlock()
		return "retry", struct{}{}
	}
}

func (sc *ShardCtrler) NotifyIfExist(index int, reply Notify) {
	_, _ = sc.DPrintf("notify index: %v", index)
	if ch, ok := sc.result[index]; ok {
		select {
		case <-sc.result[index]: // 取出之前可能遗留的apply信息
		default:
		}
		ch <- reply
	} else {
		sc.result[index] = make(chan Notify, 1)
	}
}

func (sc *ShardCtrler) getConfig(num int) Config {
	var config Config
	if num < 0 || num >= len(sc.configs) {
		config = sc.configs[len(sc.configs)-1]
	} else {
		config = sc.configs[num]
	}
	newConfig := Config{Num: config.Num, Shards: config.Shards, Groups: make(map[int][]string)}
	for gid, servers := range config.Groups {
		newConfig.Groups[gid] = append([]string{}, servers...)
	}
	return newConfig
}

func (sc *ShardCtrler) appendNewConfig(newConfig Config) {
	newConfig.Num = len(sc.configs)
	sc.configs = append(sc.configs, newConfig)
}

func (sc *ShardCtrler) isDup(ckid int64, reqid int) bool {
	v, ok := sc.ack[ckid]
	if ok {
		return v < reqid
	}
	return true
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
	close(sc.shutdown)
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me
	// 开始时，第一个配置索引为0，并且不包含group信息，让所有的shards指向该索引项
	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	sc.applyCh = make(chan raft.ApplyMsg, 100)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)
	sc.ack = make(map[int64]int)
	sc.result = make(map[int]chan Notify)
	sc.shutdown = make(chan struct{})
	// Your code here.
	go sc.checkApply()
	return sc
}

func (sc *ShardCtrler) checkApply() {
	for {
		select {
		case msg := <-sc.applyCh:
			sc.mu.Lock()
			select {
			case <-sc.shutdown:
				sc.mu.Unlock()
				return
			default:
				if msg.CommandValid {
					sc.apply(msg)
				}
				sc.mu.Unlock()
			}
		case <-sc.shutdown:
			return
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (sc *ShardCtrler) apply(msg raft.ApplyMsg) {
	reply := Notify{Op: msg.Command, Args: ""}
	switch msg.Command.(type) {
	case JoinArgs:
		joinArg, _ := msg.Command.(JoinArgs)
		if sc.isDup(joinArg.CkId, joinArg.ReqId) {
			newConfig := sc.getConfig(-1) // 获取最新的配置信息
			for gid, servers := range joinArg.Servers { // 添加新增的gid配置
				newConfig.Groups[gid] = servers
			}
			sc.modify(&newConfig) // 重新调整分片
			sc.ack[joinArg.CkId] = joinArg.ReqId
			sc.appendNewConfig(newConfig)
			reply.Args = newConfig
			_, _ = sc.DPrintf("new config: %v", newConfig)
		}
	case LeaveArgs:
		_, _ = sc.DPrintf("get leave apply msg: %v", msg)
		leaveArgs, _ := msg.Command.(LeaveArgs)
		if sc.isDup(leaveArgs.CkId, leaveArgs.ReqId) {
			newConfig := sc.getConfig(-1)
			for _, gid := range leaveArgs.GIDs {
				for shard, usedGid := range newConfig.Shards {
					if gid == usedGid {
						newConfig.Shards[shard] = 0 // 重置为0代表删除
					}
					delete(newConfig.Groups, gid)
				}
			}
			sc.modify(&newConfig) // 重新调整分片
			sc.ack[leaveArgs.CkId] = leaveArgs.ReqId
			sc.appendNewConfig(newConfig)
			reply.Args = newConfig
		}
	case MoveArgs:
		_, _ = sc.DPrintf("get move apply msg: %v", msg)
		moveArgs, _ := msg.Command.(MoveArgs)
		if sc.isDup(moveArgs.CkId, moveArgs.ReqId) {
			newConfig := sc.getConfig(-1)
			newConfig.Shards[moveArgs.Shard] = moveArgs.GID
			sc.ack[moveArgs.CkId] = moveArgs.ReqId
			sc.appendNewConfig(newConfig)
			reply.Args = newConfig
		}
	case QueryArgs:
		// 读操作，不需要应用
		queryArgs, _ := msg.Command.(QueryArgs)
		if sc.isDup(queryArgs.CkId, queryArgs.ReqId) {
			sc.ack[queryArgs.CkId] = queryArgs.ReqId
			reply.Args = sc.getConfig(queryArgs.Num)
		}
	default:
		_, _ = sc.DPrintf("unknown op: %v", msg.Command)
	}
	_, _ = sc.DPrintf("notify: %v", reply)
	sc.NotifyIfExist(msg.CommandIndex, reply)
}

// 添加或删除group配置后需要重平衡
func (sc *ShardCtrler) modify(newConfig *Config) {
	if len(newConfig.Groups) < 1 {
		return
	}
	minShardsPerGroup := NShards / len(newConfig.Groups) // 每个组最少的分片数
	if minShardsPerGroup < 1 {
		return
	}
	currentState := make(map[int][]int) // 当前 gid -> shards的状态
	currentStateSlice := make([]int, 0)
	unusedShards := make([]int, 0) // 未被使用的shards

	for gid := range newConfig.Groups {
		currentState[gid] = make([]int, 0)
		currentStateSlice = append(currentStateSlice, gid)
	}
	// 排序
	sort.Ints(currentStateSlice) // 保证遍历的顺序，map遍历无顺序
	for shard, gid := range newConfig.Shards {
		if gid != 0 {
			currentState[gid] = append(currentState[gid], shard)
		} else {
			unusedShards = append(unusedShards, shard)
		}
	}
	isBalanced := func() bool { // 判断是否所有的分片都已经使用了，并且分片分布均匀
		sum := 0
		for gid := range currentState {
			count := len(currentState[gid])
			if count < minShardsPerGroup {
				return false
			}
			sum += count
		}
		return sum == NShards
	}

	for !isBalanced() {
		for _, gid := range currentStateSlice {
			// 如果当前的group不足minShardsPerGroup并且有未使用的shards则添加从未分配的shard中添加
			for len(unusedShards) > 0 && len(currentState[gid]) < minShardsPerGroup {
				currentState[gid] = append(currentState[gid], unusedShards[0])
				unusedShards = unusedShards[1:]
			}
			// 如果当前没有未被使用的shards并且当前的group分配的shards<minShardsPerGroup,则去窃取超过minShardsPerGroup的group的shards
			if len(unusedShards) == 0 && len(currentState[gid]) < minShardsPerGroup {
				for _, gid2 := range currentStateSlice {
					if len(currentState[gid2]) > minShardsPerGroup {
						currentState[gid] = append(currentState[gid], currentState[gid2][0])
						currentState[gid2] = currentState[gid2][1:]
						break
					}
				}
			}
			// 如果还有未分配的shard则按照group的顺序一个接一个添加
			for _, gid := range currentStateSlice {
				if len(unusedShards) > 0 {
					currentState[gid] = append(currentState[gid], unusedShards[0])
					unusedShards = unusedShards[1:]
				}
			}
		}
	}
	// 按照当前的shard分配情况更新newConfig
	for gid, shards := range currentState {
		for _, shard := range shards {
			newConfig.Shards[shard] = gid
		}
	}
}
