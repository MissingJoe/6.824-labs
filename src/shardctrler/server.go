package shardctrler

import (
	"log"
	"os"
	"sort"
	"strconv"
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

type opInfo struct {
	err    Err
	config Config
}

type ShardCtrler struct {
	mu            sync.Mutex
	me            int
	rf            *raft.Raft
	applyCh       chan raft.ApplyMsg
	dead          int32
	opRecord      map[int]chan *opInfo
	clientRequest map[int64]int64
	configs       []Config // indexed by config num
}

type Op struct {
	GIDs      []int
	Servers   map[int][]string
	GID       int
	Shard     int
	Num       int
	Operation string
	ClientId  int64
	SeqId     int64
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	reply.WrongLeader = false
	sc.mu.Lock()
	if args.SeqId <= sc.clientRequest[args.ClientId] {
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()
	op := Op{
		Servers:   args.Servers,
		ClientId:  args.ClientId,
		SeqId:     args.SeqId,
		Operation: Join,
	}
	index, _, isLeader := sc.rf.Start(op)
	sc.mu.Lock()
	if !isLeader {
		////DPrintf("%v clientid %v start get in raft fail for wrong leader", sc.me, args.ClientId)
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}
	DPrintf("%v clientid %v seqId %v join server %v success start in raft", sc.me, args.ClientId&999, args.SeqId, args.Servers)
	respondCh := make(chan *opInfo, 1)
	sc.opRecord[index] = respondCh
	DPrintf("%v create respond channel index %v", sc.me, index)
	sc.mu.Unlock()
	select {
	case respond := <-respondCh:
		DPrintf("%v clientId %v seqId %v join success reply from raft", sc.me, args.ClientId&999, args.SeqId)
		reply.Err = respond.err
	case <-time.After(1000 * time.Millisecond):
		DPrintf("%v clientId %v seqId %v join in raft timeout", sc.me, args.ClientId&999, args.SeqId)
		reply.Err = TimeOut
	}
	sc.mu.Lock()
	if _, ok := sc.opRecord[index]; ok {
		delete(sc.opRecord, index)
		DPrintf("%v close respond channel clientId %v seqId %v index %v", sc.me, args.ClientId&999, args.SeqId, index)
	}
	sc.mu.Unlock()
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	reply.WrongLeader = false
	sc.mu.Lock()
	if args.SeqId <= sc.clientRequest[args.ClientId] {
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()
	op := Op{
		GIDs:      args.GIDs,
		ClientId:  args.ClientId,
		SeqId:     args.SeqId,
		Operation: Leave,
	}
	index, _, isLeader := sc.rf.Start(op)
	sc.mu.Lock()
	if !isLeader {
		////DPrintf("%v clientid %v start get in raft fail for wrong leader", sc.me, args.ClientId)
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}
	DPrintf("%v clientid %v seqId %v leave GIDs %v success start in raft", sc.me, args.ClientId&999, args.SeqId, args.GIDs)
	respondCh := make(chan *opInfo, 1)
	sc.opRecord[index] = respondCh
	DPrintf("%v create respond channel index %v", sc.me, index)
	sc.mu.Unlock()
	select {
	case respond := <-respondCh:
		DPrintf("%v clientId %v seqId %v leave success reply from raft", sc.me, args.ClientId&999, args.SeqId)
		reply.Err = respond.err
	case <-time.After(1000 * time.Millisecond):
		DPrintf("%v clientId %v seqId %v leave in raft timeout", sc.me, args.ClientId&999, args.SeqId)
		reply.Err = TimeOut
	}
	sc.mu.Lock()
	if _, ok := sc.opRecord[index]; ok {
		delete(sc.opRecord, index)
		DPrintf("%v close respond channel clientId %v seqId %v index %v", sc.me, args.ClientId&999, args.SeqId, index)
	}
	sc.mu.Unlock()
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	reply.WrongLeader = false
	sc.mu.Lock()
	if args.SeqId <= sc.clientRequest[args.ClientId] {
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()
	op := Op{
		GID:       args.GID,
		Shard:     args.Shard,
		ClientId:  args.ClientId,
		SeqId:     args.SeqId,
		Operation: Move,
	}
	index, _, isLeader := sc.rf.Start(op)
	sc.mu.Lock()
	if !isLeader {
		////DPrintf("%v clientid %v start get in raft fail for wrong leader", sc.me, args.ClientId)
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}
	DPrintf("%v clientid %v seqId %v move success start in raft", sc.me, args.ClientId&999, args.SeqId)
	respondCh := make(chan *opInfo, 1)
	sc.opRecord[index] = respondCh
	DPrintf("%v create respond channel index %v", sc.me, index)
	sc.mu.Unlock()
	select {
	case respond := <-respondCh:
		DPrintf("%v clientId %v seqId %v move success reply from raft", sc.me, args.ClientId&999, args.SeqId)
		reply.Err = respond.err
	case <-time.After(1000 * time.Millisecond):
		DPrintf("%v clientId %v seqId %v move in raft timeout", sc.me, args.ClientId&999, args.SeqId)
		reply.Err = TimeOut
	}
	sc.mu.Lock()
	if _, ok := sc.opRecord[index]; ok {
		delete(sc.opRecord, index)
		DPrintf("%v close respond channel clientId %v seqId %v index %v", sc.me, args.ClientId&999, args.SeqId, index)
	}
	sc.mu.Unlock()
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	reply.WrongLeader = false
	op := Op{
		Num:       args.Num,
		ClientId:  args.ClientId,
		SeqId:     args.SeqId,
		Operation: Query,
	}
	index, _, isLeader := sc.rf.Start(op)
	sc.mu.Lock()
	if !isLeader {
		////DPrintf("%v clientid %v start get in raft fail for wrong leader", sc.me, args.ClientId)
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}
	DPrintf("%v clientid %v seqId %v query Num %v success start in raft", sc.me, args.ClientId&999, args.SeqId, args.Num)
	respondCh := make(chan *opInfo, 1)
	sc.opRecord[index] = respondCh
	DPrintf("%v create respond channel index %v", sc.me, index)
	sc.mu.Unlock()
	select {
	case respond := <-respondCh:
		DPrintf("%v clientId %v seqId %v query success reply from raft", sc.me, args.ClientId&999, args.SeqId)
		reply.Err, reply.Config = respond.err, respond.config
	case <-time.After(1000 * time.Millisecond):
		DPrintf("%v clientId %v seqId %v query in raft timeout", sc.me, args.ClientId&999, args.SeqId)
		reply.Err = TimeOut
	}
	sc.mu.Lock()
	if _, ok := sc.opRecord[index]; ok {
		delete(sc.opRecord, index)
		DPrintf("%v close respond channel clientId %v seqId %v index %v", sc.me, args.ClientId&999, args.SeqId, index)
	}
	sc.mu.Unlock()
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func getMaxLoadGid(gidLoad map[int][]int) int {
	var keys []int
	for k := range gidLoad {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	index, max := 0, -1
	for _, key := range keys {
		if _, ok := gidLoad[key]; ok {
			if len(gidLoad[key]) > max {
				index, max = key, len(gidLoad[key])
			}
		}
	}
	return index
}

func getMinLoadGid(gidLoad map[int][]int) int {
	var keys []int
	for k := range gidLoad {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	index, min := 0, NShards+1
	for _, key := range keys {
		if _, ok := gidLoad[key]; ok {
			if len(gidLoad[key]) < min {
				index, min = key, len(gidLoad[key])
			}
		} else {
			index = key
			break
		}
	}
	return index
}

func inSlice(num int, slice []int) bool {
	for i := 0; i < len(slice); i++ {
		if slice[i] == num {
			return true
		}
	}
	return false
}

func (sc *ShardCtrler) executeJoin(servers map[int][]string) {
	lastConfig := sc.configs[len(sc.configs)-1]
	config := Config{}
	config.Num = lastConfig.Num + 1
	config.Groups = make(map[int][]string, len(lastConfig.Groups))
	gidList := make([]int, 0)
	for k, v := range lastConfig.Groups {
		if _, ok := lastConfig.Groups[k]; ok {
			server := make([]string, len(v))
			copy(server, v)
			config.Groups[k] = server
			gidList = append(gidList, k)
		}
	}
	// add servers
	for k, v := range servers {
		if _, ok := config.Groups[k]; !ok {
			server := make([]string, len(v))
			copy(server, v)
			config.Groups[k] = server
			gidList = append(gidList, k)
		}
	}
	// load balance
	sort.Ints(gidList)
	gidLoad := make(map[int][]int, len(gidList))
	for _, gid := range gidList {
		gidLoad[gid] = make([]int, 0)
	}
	j := 0
	for ; j < NShards; j++ {
		if lastConfig.Shards[j] != 0 {
			break
		}
	}
	for i := 0; i < NShards; i++ {
		if j == NShards {
			gidLoad[gidList[0]] = append(gidLoad[gidList[0]], i)
		} else {
			gidLoad[lastConfig.Shards[i]] = append(gidLoad[lastConfig.Shards[i]], i)
		}
	}
	for {
		source, target := getMaxLoadGid(gidLoad), getMinLoadGid(gidLoad)
		if _, ok := gidLoad[target]; ok {
			if len(gidLoad[source])-len(gidLoad[target]) <= 1 {
				break
			}
		}
		gidLoad[target] = append(gidLoad[target], gidLoad[source][0])
		gidLoad[source] = gidLoad[source][1:]
	}
	DPrintf("%v gidLoad %v", sc.me, gidLoad)
	for gid, load := range gidLoad {
		for _, v := range load {
			config.Shards[v] = gid
		}
	}
	sc.configs = append(sc.configs, config)
}

func (sc *ShardCtrler) executeLeave(GIDs []int) {
	lastConfig := sc.configs[len(sc.configs)-1]
	config := Config{}
	config.Num = lastConfig.Num + 1
	config.Groups = make(map[int][]string, len(lastConfig.Groups))
	gidListNew := make([]int, 0)
	for k, v := range lastConfig.Groups {
		if !inSlice(k, GIDs) {
			server := make([]string, len(v))
			copy(server, v)
			config.Groups[k] = server
			gidListNew = append(gidListNew, k)
		}
	}
	sort.Ints(GIDs)
	sort.Ints(gidListNew)
	gidLoadNew := make(map[int][]int, len(gidListNew))
	gidLoadDelete := make(map[int][]int, len(GIDs))
	for _, gid := range gidListNew {
		gidLoadNew[gid] = make([]int, 0)
	}
	for i := 0; i < NShards; i++ {
		if inSlice(lastConfig.Shards[i], GIDs) {
			gidLoadDelete[lastConfig.Shards[i]] = append(gidLoadDelete[lastConfig.Shards[i]], i)
		} else {
			gidLoadNew[lastConfig.Shards[i]] = append(gidLoadNew[lastConfig.Shards[i]], i)
		}
	}
	//delete GIDs and load balance
	for _, gid := range GIDs {
		appendIndex := 0
		for appendIndex < len(gidLoadDelete[gid]) {
			source := getMinLoadGid(gidLoadNew)
			gidLoadNew[source] = append(gidLoadNew[source], gidLoadDelete[gid][appendIndex])
			appendIndex++
		}
	}
	DPrintf("%v gidLoad %v", sc.me, gidLoadNew)
	for gid, load := range gidLoadNew {
		for _, v := range load {
			config.Shards[v] = gid
		}
	}
	sc.configs = append(sc.configs, config)
}

func (sc *ShardCtrler) executeMove(GID int, Shard int) {
	lastConfig := sc.configs[len(sc.configs)-1]
	config := Config{}
	config.Num = lastConfig.Num + 1
	config.Shards = lastConfig.Shards
	config.Shards[Shard] = GID
	config.Groups = make(map[int][]string, len(lastConfig.Groups))
	for k, v := range lastConfig.Groups {
		server := make([]string, len(v))
		copy(server, v)
		config.Groups[k] = server

	}
	sc.configs = append(sc.configs, config)
}

func (sc *ShardCtrler) executeQuery(num int) Config {
	if num == -1 || num > len(sc.configs)-1 {
		return sc.configs[len(sc.configs)-1]
	} else {
		return sc.configs[num]
	}
}

func (sc *ShardCtrler) isDuplicateRequest(clientId int64, seqId int64) bool {
	if id, ok := sc.clientRequest[clientId]; ok && seqId <= id {
		return true
	}
	return false
}

func (sc *ShardCtrler) applier() {
	for sc.killed() == false {
		select {
		case msg := <-sc.applyCh:
			if msg.CommandValid {
				command := msg.Command.(Op)
				respond := opInfo{}
				DPrintf("%v recevie msg from raft commitIndex %v opIndex %v", sc.me, msg.CommandIndex, command.SeqId)
				switch command.Operation {
				case Move:
					sc.mu.Lock()
					if sc.isDuplicateRequest(command.ClientId, command.SeqId) {
						DPrintf("%v recive msg clientId %v seqid %v is smaller than me %v", sc.me, command.ClientId&999, command.SeqId, sc.clientRequest[command.ClientId])
						respond.err = OK
						break
					}
					DPrintf("%v clientId %v seqid %v start move", sc.me, command.ClientId&999, command.SeqId)
					sc.executeMove(command.GID, command.Shard)
					sc.clientRequest[command.ClientId] = command.SeqId
					DPrintf("%v clientId %v seqid %v move sucess", sc.me, command.ClientId&999, command.SeqId)
					DPrintf("%v shard is %v", sc.me, sc.configs[len(sc.configs)-1].Shards)
					sc.mu.Unlock()
				case Leave:
					sc.mu.Lock()
					if sc.isDuplicateRequest(command.ClientId, command.SeqId) {
						DPrintf("%v recive msg clientId %v seqid %v is smaller than me %v", sc.me, command.ClientId&999, command.SeqId, sc.clientRequest[command.ClientId])
						respond.err = OK
						break
					}
					DPrintf("%v clientId %v seqid %v start leave %v", sc.me, command.ClientId&999, command.SeqId, command.GIDs)
					sc.executeLeave(command.GIDs)
					sc.clientRequest[command.ClientId] = command.SeqId
					DPrintf("%v clientId %v seqid %v leave %v sucess", sc.me, command.ClientId&999, command.SeqId, command.GIDs)
					DPrintf("%v shard is %v", sc.me, sc.configs[len(sc.configs)-1].Shards)
					s := "["
					for k := range sc.configs[len(sc.configs)-1].Groups {
						s += strconv.Itoa(k)
						s += " "
					}
					s += "]"
					DPrintf("%v Group is %v", sc.me, s)
					sc.mu.Unlock()
				case Query:
					sc.mu.Lock()
					respond.config = sc.executeQuery(command.Num)
					//sc.clientRequest[command.ClientId] = command.SeqId
					DPrintf("%v clientId %v seqid %v query %v sucess", sc.me, command.ClientId&999, command.SeqId, command.Num)
					DPrintf("%v shard is %v", sc.me, sc.configs[len(sc.configs)-1].Shards)
					s := "["
					for k := range sc.configs[len(sc.configs)-1].Groups {
						s += strconv.Itoa(k)
						s += " "
					}
					s += "]"
					DPrintf("%v Group is %v", sc.me, s)

					sc.mu.Unlock()
				case Join:
					sc.mu.Lock()
					if sc.isDuplicateRequest(command.ClientId, command.SeqId) {
						DPrintf("%v recive msg clientId %v seqid %v is smaller than me %v", sc.me, command.ClientId&999, command.SeqId, sc.clientRequest[command.ClientId])
						respond.err = OK
						break
					}
					DPrintf("%v clientId %v seqid %v start Join %v", sc.me, command.ClientId&999, command.SeqId, command.Servers)
					sc.executeJoin(command.Servers)
					sc.clientRequest[command.ClientId] = command.SeqId
					DPrintf("%v clientId %v seqid %v Join %v sucess", sc.me, command.ClientId&999, command.SeqId, command.Servers)
					DPrintf("%v shard is %v", sc.me, sc.configs[len(sc.configs)-1].Shards)
					s := "["
					for k := range sc.configs[len(sc.configs)-1].Groups {
						s += strconv.Itoa(k)
						s += " "
					}
					s += "]"
					DPrintf("%v Group is %v", sc.me, s)
					sc.mu.Unlock()
				}
				sc.mu.Lock()
				opRespondCh, opRespondOK := sc.opRecord[msg.CommandIndex]
				if opRespondOK {
					if term, isleader := sc.rf.GetState(); isleader && term == msg.CommandTerm {
						opRespondCh <- &respond
					} else {
						DPrintf("%v term dismatch index %v currentTerm %v commandTerm %v", sc.me, msg.CommandIndex, term, msg.CommandTerm)
					}
				} else {
					DPrintf("%v respond channel is closed index %v", sc.me, msg.CommandIndex)
				}
				sc.mu.Unlock()
			}
		}
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me
	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}
	sc.configs[0].Num = 0
	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)
	sc.opRecord = make(map[int]chan *opInfo)
	sc.clientRequest = make(map[int64]int64)
	go sc.applier()
	return sc
}
