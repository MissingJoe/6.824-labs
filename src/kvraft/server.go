package kvraft

import (
	"bytes"
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
	lastApplied   int
	database      map[string]string
	opRecord      map[int]chan *opInfo
	clientRequest map[int64]int64
}

func (kv *KVServer) createSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.database)
	e.Encode(kv.clientRequest)
	return w.Bytes()
}

func (kv *KVServer) readSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) == 0 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var database map[string]string
	var clientRequest map[int64]int64
	if d.Decode(&database) != nil {
		////DPrintf("%v error in decode current term", rf.me)
	} else if d.Decode(&clientRequest) != nil {
		////DPrintf("%v error in decode votefor", rf.me)
	} else {
		kv.database = database
		kv.clientRequest = clientRequest
	}
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
		////DPrintf("%v clientid %v start get in raft fail for wrong leader", kv.me, args.ClientId)
		reply.Err = ErrWrongLeader
		return
	}
	//DPrintf("%v clientid %v seqId %v op %v key %v get success start in raft", kv.me, args.ClientId&999, args.SeqId, "Get", args.Key)
	respondCh := make(chan *opInfo, 1)
	kv.mu.Lock()
	kv.opRecord[index] = respondCh
	kv.mu.Unlock()
	select {
	case respond := <-respondCh:
		//DPrintf("%v clientid %v seqId %v get success reply from raft", kv.me, args.ClientId&999, args.SeqId)
		reply.Err, reply.Value = respond.err, respond.value
	case <-time.After(1000 * time.Millisecond):
		//DPrintf("%v clientid %v seqId %v get in raft timeout", kv.me, args.ClientId&999, args.SeqId)
		reply.Err = TimeOut
	}
	kv.mu.Lock()
	if _, ok := kv.opRecord[index]; ok {
		close(respondCh)
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
		////DPrintf("%v clientid %v start putappend in raft fail for wrong leader", kv.me, args.ClientId)
		reply.Err = ErrWrongLeader
		return
	}
	//DPrintf("%v clientid %v seqId %v op %v key %v putappend success start in raft", kv.me, args.ClientId&999, args.SeqId, args.Op, args.Key)
	respondCh := make(chan *opInfo, 1)
	kv.mu.Lock()
	kv.opRecord[index] = respondCh
	kv.mu.Unlock()
	select {
	case respond := <-respondCh:
		//DPrintf("%v clientid %v seqId %v putappend success reply from raft", kv.me, args.ClientId&999, args.SeqId)
		reply.Err = respond.err
	case <-time.After(1000 * time.Millisecond):
		//DPrintf("%v clientid %v seqId %v putappend in raft timeout", kv.me, args.ClientId&999, args.SeqId)
		reply.Err = TimeOut
	}
	kv.mu.Lock()
	if _, ok := kv.opRecord[index]; ok {
		close(respondCh)
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

func (kv *KVServer) printDatabase() {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	s := ""
	for k, v := range kv.database {
		s += k
		s += ":"
		s += v
		s += ", "
	}
	//DPrintf("%v state machine is: %v", kv.me, s)
}

func (kv *KVServer) listen() {
	for kv.killed() == false {
		select {
		case msg := <-kv.applyCh:
			if msg.CommandValid {
				command := msg.Command.(Op)
				//DPrintf("%v recive command apply from raft index %v op %v key %v", kv.me, msg.CommandIndex, command.Operation, command.Key)
				kv.mu.Lock()
				if kv.lastApplied >= msg.CommandIndex {
					//DPrintf("%v commandIndex is out of date because of snapshot", kv.me)
					kv.mu.Unlock()
					continue
				}
				kv.lastApplied = msg.CommandIndex
				respond := opInfo{}
				if command.Operation == "Get" {
					value, ok := kv.database[command.Key]
					if ok {
						respond.value = value
						respond.err = OK
					} else {
						respond.err = ErrNoKey
					}
				} else {
					if seqId, ok := kv.clientRequest[command.ClientId]; ok && command.SeqId <= seqId {
						//DPrintf("%v recive msg clientId %v seqid %v is smaller than me %v", kv.me, command.ClientId&999, command.SeqId, kv.clientRequest[command.ClientId])
						respond.err = OK
					} else {
						if command.Operation == "Put" {
							kv.database[command.Key] = command.Value
						} else {
							kv.database[command.Key] += command.Value
						}
						respond.err = OK
						kv.clientRequest[command.ClientId] = command.SeqId
					}
				}
				opRespondCh, opRespondOK := kv.opRecord[msg.CommandIndex]
				if opRespondOK {
					if term, isleader := kv.rf.GetState(); isleader && term == msg.CommandTerm {
						opRespondCh <- &respond
					}
				}
				if kv.maxraftstate > 0 && kv.rf.GetRaftStateSize() > kv.maxraftstate {
					kv.rf.Snapshot(msg.CommandIndex, kv.createSnapshot())
					//DPrintf("%v generate snapshot in commandIndex %v", kv.me, msg.CommandIndex)
				}
				kv.mu.Unlock()
			} else if msg.SnapshotValid {
				//DPrintf("%v recive snapshot apply from raft index %v", kv.me, msg.SnapshotIndex)
				kv.mu.Lock()
				if len(msg.Snapshot) > 0 && kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
					kv.readSnapshot(msg.Snapshot)
					kv.lastApplied = msg.SnapshotIndex
					//DPrintf("%v install snapshot index %v", kv.me, msg.SnapshotIndex)
				}
				kv.mu.Unlock()
			} else {

			}
		}
	}
}

func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.lastApplied = 0
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.opRecord = make(map[int]chan *opInfo)
	kv.database = make(map[string]string)
	kv.clientRequest = make(map[int64]int64)
	kv.readSnapshot(persister.ReadSnapshot())
	//DPrintf("%v read snapshot", kv.me)
	//kv.printDatabase()
	go kv.listen()
	return kv
}
