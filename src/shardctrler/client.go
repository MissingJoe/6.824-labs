package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

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

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	args.Num = num
	args.ClientId = ck.clientId
	args.SeqId = atomic.AddInt64(&ck.seqId, 1)
	for {
		// try each known server.
		ck.mu.Lock()
		leader := ck.leaderId
		ck.mu.Unlock()
		var reply QueryReply
		ok := ck.servers[leader].Call("ShardCtrler.Query", args, &reply)
		if ok && reply.WrongLeader == false {
			return reply.Config
		}
		ck.mu.Lock()
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		ck.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers
	args.ClientId = ck.clientId
	args.SeqId = atomic.AddInt64(&ck.seqId, 1)
	for {
		// try each known server.
		ck.mu.Lock()
		leader := ck.leaderId
		ck.mu.Unlock()
		var reply JoinReply
		ok := ck.servers[leader].Call("ShardCtrler.Join", args, &reply)
		if ok && reply.WrongLeader == false {
			return
		}
		ck.mu.Lock()
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		ck.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids
	args.ClientId = ck.clientId
	args.SeqId = atomic.AddInt64(&ck.seqId, 1)
	for {
		ck.mu.Lock()
		leader := ck.leaderId
		ck.mu.Unlock()
		var reply QueryReply
		ok := ck.servers[leader].Call("ShardCtrler.Leave", args, &reply)
		if ok && reply.WrongLeader == false {
			return
		}
		ck.mu.Lock()
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		ck.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	args.ClientId = ck.clientId
	args.SeqId = atomic.AddInt64(&ck.seqId, 1)
	for {
		ck.mu.Lock()
		leader := ck.leaderId
		ck.mu.Unlock()
		var reply MoveReply
		ok := ck.servers[leader].Call("ShardCtrler.Move", args, &reply)
		if ok && reply.WrongLeader == false {
			return
		}
		ck.mu.Lock()
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		ck.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}
