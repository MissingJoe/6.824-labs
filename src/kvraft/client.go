package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync/atomic"
	"time"

	"sync"

	"6.824/labrpc"
)

type Clerk struct {
	servers  []*labrpc.ClientEnd
	mu       sync.Mutex
	leaderId int
	clientId int64
	seqId    int64
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
	ck.clientId = nrand()
	ck.seqId = 0
	ck.leaderId = 0
	return ck
}

func (ck *Clerk) Get(key string) string {
	args := GetArgs{
		Key:      key,
		ClientId: ck.clientId,
		SeqId:    atomic.AddInt64(&ck.seqId, 1),
	}
	reply := GetReply{}
	for {
		ck.mu.Lock()
		leader := ck.leaderId
		ck.mu.Unlock()
		ok := ck.servers[leader].Call("KVServer.Get", &args, &reply)
		if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
			DPrintf("%v success get with key %v seq %v leader %v", ck.clientId, key, args.SeqId, leader)
			break
		}
		//DPrintf("%v get with key %v seq %v leader %v", ck.clientId, key, args.SeqId, leader)
		ck.mu.Lock()
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		reply = GetReply{}
		ck.mu.Unlock()
		time.Sleep(time.Millisecond)
	}
	return reply.Value
}

func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClientId: ck.clientId,
		SeqId:    atomic.AddInt64(&ck.seqId, 1),
	}
	for {
		ck.mu.Lock()
		leader := ck.leaderId
		ck.mu.Unlock()
		reply := PutAppendReply{}
		// if args.Op == "Put" {
		// 	DPrintf("%v put with key %v value %v seq %v", ck.clientId, key, value, args.SeqId)
		// } else {
		// 	DPrintf("%v append with key %v value %v seq %v", ck.clientId, key, value, args.SeqId)
		// }
		ok := ck.servers[leader].Call("KVServer.PutAppend", &args, &reply)
		if ok && reply.Err == OK {
			if args.Op == "Put" {
				DPrintf("%v put with key %v value %v seq %v leader %v", ck.clientId, key, value, args.SeqId, leader)
			} else {
				DPrintf("%v append with key %v value %v seq %v leader %v", ck.clientId, key, value, args.SeqId, leader)
			}
			break
		}
		ck.mu.Lock()
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		ck.mu.Unlock()
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
