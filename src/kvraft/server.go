package kvraft

import (
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	log.SetFlags(log.Lmicroseconds)
	f, err := os.OpenFile("log.log", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0777)
	if err != nil {
		return
	}
	defer func() {
		f.Close()
	}()

	log.SetOutput(f)
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	Key       string
	Value     string
	Operation string
	ClientId  int64
	SeqId     int64
}

type opInfo struct {
	err   Err
	value string
}

type KVServer struct {
	mu            sync.Mutex
	me            int
	rf            *raft.Raft
	applyCh       chan raft.ApplyMsg
	dead          int32 // set by Kill()
	maxraftstate  int   // snapshot if log grows this big
	stateMachine  map[string]string
	opRecord      map[int]chan *opInfo
	clientRequest map[int64]int64
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	op := &Op{
		Key:       args.Key,
		Operation: "Get",
		ClientId:  args.ClientId,
		SeqId:     args.SeqId,
	}
	index, _, isLeader := kv.rf.Start(*op)
	if !isLeader {
		//DPrintf("%v clientid %v start get in raft fail for wrong leader", kv.me, args.ClientId)
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("%v clientid %v start get in raft", kv.me, args.ClientId)
	respondCh := make(chan *opInfo, 1)
	kv.mu.Lock()
	kv.opRecord[index] = respondCh
	kv.mu.Unlock()
	select {
	case respond := <-respondCh:
		DPrintf("%v clientid %v get success reply from raft", kv.me, args.ClientId)
		reply.Err, reply.Value = respond.err, respond.value
	case <-time.After(1000 * time.Millisecond):
		DPrintf("%v clientid %v get in raft timeout resent", kv.me, args.ClientId)
		reply.Err = TimeOut
	}
	kv.mu.Lock()
	if _, ok := kv.opRecord[index]; ok {
		delete(kv.opRecord, index)
	}
	kv.mu.Unlock()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	if args.SeqId <= kv.clientRequest[args.ClientId] {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	op := &Op{
		Key:       args.Key,
		Value:     args.Value,
		Operation: args.Op,
		ClientId:  args.ClientId,
		SeqId:     args.SeqId,
	}
	index, _, isLeader := kv.rf.Start(*op)
	if !isLeader {
		//DPrintf("%v clientid %v start putappend in raft fail for wrong leader", kv.me, args.ClientId)
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("%v clientid %v putappend success start in raft", kv.me, args.ClientId)
	respondCh := make(chan *opInfo, 1)
	kv.mu.Lock()
	kv.opRecord[index] = respondCh
	kv.mu.Unlock()
	select {
	case respond := <-respondCh:
		DPrintf("%v clientid %v putappend success reply from raft", kv.me, args.ClientId)
		reply.Err = respond.err
	case <-time.After(1000 * time.Millisecond):
		DPrintf("%v clientid %v putappend in raft timeout resent", kv.me, args.ClientId)
		reply.Err = TimeOut
	}
	kv.mu.Lock()
	if _, ok := kv.opRecord[index]; ok {
		delete(kv.opRecord, index)
	}
	kv.mu.Unlock()
}

func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) listen() {
	for kv.killed() == false {
		select {
		case msg := <-kv.applyCh:
			if msg.CommandValid {
				command := msg.Command.(Op)
				kv.mu.Lock()
				respond := opInfo{}
				if command.Operation == "Get" {
					value, ok := kv.stateMachine[command.Key]
					if ok {
						respond.value = value
						respond.err = OK
					} else {
						respond.err = ErrNoKey
					}
					DPrintf("%v get value success key %v", kv.me, command.Key)
					kv.clientRequest[command.ClientId] = command.SeqId
				} else {
					if command.SeqId <= kv.clientRequest[command.ClientId] {
						DPrintf("%v recive msg seqid %v is smaller than me %v", kv.me, command.SeqId, kv.clientRequest[command.ClientId])
						respond.err = OK
					} else {
						if command.Operation == "Put" {
							kv.stateMachine[command.Key] = command.Value
							DPrintf("%v put value success key %v value %v", kv.me, command.Key, command.Value)
						} else {
							kv.stateMachine[command.Key] += command.Value
							DPrintf("%v append value success key %v value %v", kv.me, command.Key, command.Value)
						}
						respond.err = OK
						kv.clientRequest[command.ClientId] = command.SeqId
					}
				}
				s := ""
				for k, v := range kv.stateMachine {
					s += k
					s += ":"
					s += v
					s += ", "
				}
				DPrintf("%v state machine is: %v", kv.me, s)
				opRespondCh, opRespondOK := kv.opRecord[msg.CommandIndex]
				if opRespondOK {
					if term, isleader := kv.rf.GetState(); isleader && term == msg.CommandTerm {
						opRespondCh <- &respond
					}
				}
				kv.mu.Unlock()
			} else {

			}
		}
	}
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
	kv.maxraftstate = maxraftstate
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.opRecord = make(map[int]chan *opInfo)
	kv.stateMachine = make(map[string]string)
	kv.clientRequest = make(map[int64]int64)
	go kv.listen()
	return kv
}
