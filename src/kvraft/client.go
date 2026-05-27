package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"

	"6.824/src/labrpc"
)

type Clerk struct {
	mu sync.Mutex
	servers []*labrpc.ClientEnd
	requestId int64
	clientId  int64
	leaderId  int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.mu = sync.Mutex{}

	ck.servers = servers
	
	ck.clientId = nrand()
	ck.requestId = 0
	ck.leaderId = 0

	return ck
}

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
func (ck *Clerk) Get(key string) string {
	ck.mu.Lock()
	ck.requestId++
	
	args := GetArgs{
		Key:       key,
		ClientId:  ck.clientId,
		RequestId: ck.requestId,
	}

	ck.mu.Unlock()

	reply := GetReply{}

	for {
		if !ck.servers[ck.leaderId].Call("KVServer.Get", &args, &reply) || reply.Err == ErrWrongLeader {
			ck.mu.Lock()
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			ck.mu.Unlock()
		} else if reply.Err == OK {
			return reply.Value
		}
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.mu.Lock()
	ck.requestId++
	
	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		ClientId:  ck.clientId,
		RequestId: ck.requestId,
	}

	ck.mu.Unlock()

	reply := PutAppendReply{}

	for {
		if !ck.servers[ck.leaderId].Call("KVServer.PutAppend", &args, &reply) || reply.Err == ErrWrongLeader {
			ck.mu.Lock()
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			ck.mu.Unlock()
		} else if reply.Err == OK {
			return
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, PUT)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, APPEND)
}
