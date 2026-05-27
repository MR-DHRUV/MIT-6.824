package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/src/kvstore"
	"6.824/src/labgob"
	"6.824/src/labrpc"
	"6.824/src/raft"
)

const TIMEOUT = 800 * time.Millisecond

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	Type      string
	Key       string
	Value     string
	ClientId  int64
	RequestId int64
}

type Result struct {
	Err   Err
	Value string
}

type ResponseContext struct {
	RequestId int64
	Result    Result
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32

	// map command index to a channel that the result will be sent to
	resultChan map[int]chan Result

	// map client id to the sequence number of the latest request
	clientSeqMap map[int64]ResponseContext

	// map command index to the op that is waiting for it to be applied
	// In raft uncommited indices could be rewwriten by new leaders
	// this ensures reply is delivered to the right client for the right request
	waitingMap map[int]Op

	kvStore *kvstore.KVStore

	// index of the current command to be applied
	commandIndex int

	maxRaftStateBytes int
}

func (kv *KVServer) applyLoop() {
	for msg := range kv.applyCh {
		if kv.killed() {
			return
		}

		if !msg.CommandValid {
			continue
		}

		kv.mu.Lock()

		res := Result{}
		cmd := msg.Command.(Op)

		// Check for duplicate before applying mutations
		isDuplicate, cachedRes := kv.checkDuplicate(cmd)

		if isDuplicate {
			res = cachedRes
		} else {
			switch cmd.Type {
			case PUT:
				kv.kvStore.Put(cmd.Key, cmd.Value)
				res.Err = OK
			case APPEND:
				kv.kvStore.Append(cmd.Key, cmd.Value)
				res.Err = OK
			case GET:
				value, ok := kv.kvStore.Get(cmd.Key)
				if ok {
					res.Err = OK
					res.Value = value
				} else {
					res.Err = ErrNoKey
					res.Value = ""
				}
			}

			// Only cache results for mutating operations
			if cmd.Type != GET {
				kv.clientSeqMap[cmd.ClientId] = ResponseContext{
					RequestId: cmd.RequestId,
					Result:    res,
				}
			}
		}

		oper, isWaiting := kv.waitingMap[msg.CommandIndex]
		resCh, ok := kv.resultChan[msg.CommandIndex]

		kv.mu.Unlock()

		// lock should not be held while sending to channel
		// should verify for correct reciever
		if ok && isWaiting && oper.ClientId == cmd.ClientId && oper.RequestId == cmd.RequestId {
			resCh <- res
		}
	}
}

func (kv *KVServer) rpcCleanup(idx int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	delete(kv.resultChan, idx)
	delete(kv.waitingMap, idx)
}

// The caller must hold the lock
func (kv *KVServer) checkDuplicate(op Op) (bool, Result) {
	lastOp, ok := kv.clientSeqMap[op.ClientId]

	if ok && op.RequestId <= lastOp.RequestId {
		return true, lastOp.Result
	}

	return false, Result{}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()

	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}

	op := Op{
		Type:      args.Op,
		Key:       args.Key,
		Value:     args.Value,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}

	// check for duplicate req
	isDuplicate, res := kv.checkDuplicate(op)
	if isDuplicate {
		reply.Err = res.Err
		kv.mu.Unlock()
		return
	}

	idx, _, ok := kv.rf.Start(op)

	if !ok {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}

	kv.waitingMap[idx] = op

	resultChan := make(chan Result, 1) // buffered for safety against accidental blocking
	kv.resultChan[idx] = resultChan

	kv.mu.Unlock()

	select {
	case res := <-resultChan:
		reply.Err = res.Err
	case <-time.After(TIMEOUT):
		reply.Err = ErrServerTimeout
	}

	// clean up
	go kv.rpcCleanup(idx)
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()

	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}

	op := Op{
		Type:      GET,
		Key:       args.Key,
		Value:     "",
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}

	idx, _, ok := kv.rf.Start(op)

	if !ok {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}

	kv.waitingMap[idx] = op

	resultChan := make(chan Result, 1) // buffered for safety against accidental blocking
	kv.resultChan[idx] = resultChan

	kv.mu.Unlock()

	select {
	case res := <-resultChan:
		reply.Err = res.Err
		reply.Value = res.Value
	case <-time.After(TIMEOUT):
		reply.Err = ErrServerTimeout
	}

	// clean up
	go kv.rpcCleanup(idx)
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

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
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.commandIndex = 0
	kv.maxRaftStateBytes = maxraftstate

	// You may need initialization code here.
	kv.kvStore = kvstore.NewKVStore()
	kv.resultChan = make(map[int]chan Result)
	kv.clientSeqMap = make(map[int64]ResponseContext)
	kv.waitingMap = make(map[int]Op)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.applyLoop()

	return kv
}
