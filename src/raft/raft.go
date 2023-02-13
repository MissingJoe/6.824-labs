package raft

// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.

import (
	//	"bytes"

	"sync"
	"sync/atomic"

	//	"6.824/labgob"

	"math/rand"
	"time"

	"6.824/labrpc"

	"strconv"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Entry struct {
	Command interface{}
	Term    int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	state         int
	currentTerm   int
	votedFor      int
	log           []*Entry
	applyChan     chan ApplyMsg
	applyCond     *sync.Cond
	elctionTime   time.Duration
	lastTimeStamp time.Time
	commitIndex   int
	lastApplied   int
	nextIndex     []int
	matchIndex    []int
}

const (
	fellower  = 0
	candidate = 1
	leader    = 2
)

func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	if rf.state == leader {
		isleader = true
	} else {
		isleader = false
	}
	rf.mu.Unlock()
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

func (rf *Raft) SetElectionTime() {
	rand.Seed(time.Now().UnixNano())
	rf.elctionTime = time.Duration(rand.Intn(400)+400) * time.Millisecond
	//DPrintf("%v election time is %v\n", rf.me, rf.elctionTime)
}

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidatedId int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.VoteGranted = false
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		//DPrintf("%v term %v is smaller than my term %v\n", rf.me, args.Term, rf.currentTerm)
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		if rf.state != fellower {
			//DPrintf("%v is recive big term so become fellower\n", rf.me)
			rf.state = fellower
		}
		rf.votedFor = -1
	}
	if rf.votedFor == -1 || rf.votedFor == args.CandidatedId {
		lastLogIndex := len(rf.log) - 1
		lastTerm := -1
		if lastLogIndex >= 0 {
			lastTerm = rf.log[lastLogIndex].Term
		}
		////DPrintf("%v lastlogindex:%v, lastterm:%v, argsindex:%v, argsterm:%v", rf.me, lastLogIndex, lastTerm, args.LastLogIndex, args.LastLogTerm)
		if lastTerm < args.LastLogTerm || (lastTerm == args.LastLogTerm && args.LastLogIndex >= lastLogIndex) {
			reply.VoteGranted = true
		} else {
			return
		}
		//DPrintf("%v vote for %v in term %v\n", rf.me, args.CandidatedId, rf.currentTerm)
		rf.votedFor = args.CandidatedId
		rf.lastTimeStamp = time.Now()
	} else {
		//DPrintf("%v already vote for %v and wait for heartbeat\n", rf.me, rf.votedFor)
	}
}

// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// RPC AppendEntry
type AppendEntryArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []*Entry
	LeaderCommit int
}

type AppendEntryReply struct {
	Term    int
	Success bool
	XTerm   int
	XIndex  int
	XLen    int
}

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Success = false
	reply.Term = rf.currentTerm
	//DPrintf("%v recive heartbeat from %v with entry length %v\n", rf.me, args.LeaderId, len(args.Entries))
	if args.Term < rf.currentTerm {
		//DPrintf("%v term %v is smaller than my term %v\n", rf.me, args.Term, rf.currentTerm)
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		if rf.state != fellower {
			//DPrintf("%v is recive big term so become fellower\n", rf.me)
			rf.state = fellower
		}
		rf.votedFor = -1
	}
	rf.lastTimeStamp = time.Now()

	if args.PrevLogIndex == -1 {
		// fisrt heartbeat
		reply.Success = true
	} else {
		if args.PrevLogIndex > len(rf.log)-1 {
			//DPrintf("%v recive appendentry PrevLogIndex %v is empty\n", rf.me, args.PrevLogIndex)
			reply.XTerm = -99
			reply.XLen = 0
			for i := args.PrevLogIndex; i >= len(rf.log); i-- {
				reply.XLen++
			}
			//DPrintf("%v recive appendentry xlen is %v\n", rf.me, reply.XLen)
			return
		}
		if rf.log[args.PrevLogIndex].Term == args.PrevLogTerm {
			if len(args.Entries) != 0 {
				rf.log = rf.log[:args.PrevLogIndex+1]
				rf.log = append(rf.log, args.Entries...)
				s := ""
				for i, v := range rf.log {
					s = s + strconv.Itoa(i) + ":" + strconv.Itoa(v.Term)
					s += " "
				}
				//DPrintf("%v log is: %v", rf.me, s)
			}
			//DPrintf("%v match preindex %v and term %v\n", rf.me, args.PrevLogIndex, args.PrevLogTerm)
			reply.Success = true
		} else {
			//DPrintf("%v don't match preindex %v and term %v\n", rf.me, args.PrevLogIndex, args.PrevLogTerm)
			reply.XTerm = rf.log[args.PrevLogIndex].Term
			i := args.PrevLogIndex
			for ; rf.log[i].Term == reply.XTerm && i >= 1; i-- {
			}
			reply.XIndex = i + 1
			rf.log = rf.log[0:args.PrevLogIndex]
			//DPrintf("%v don't match preindex xterm is %v, xindex is %v\n", rf.me, reply.XTerm, reply.XIndex)
			return
		}
	}
	//check leadercommit
	if args.LeaderCommit > rf.commitIndex {
		//commit entry
		if args.LeaderCommit <= len(rf.log)-1 {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = len(rf.log) - 1
		}
		rf.applyCond.Broadcast()
	}
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	if rf.state != leader {
		isLeader = false
		rf.mu.Unlock()
		////DPrintf("%v use Start fail for unleader in index %v and term %v", rf.me, index, term)
		return index, term, isLeader
	}
	term = rf.currentTerm
	entry := Entry{
		Command: command,
		Term:    term,
	}
	////DPrintf("start without append log length is %v", len(rf.log))
	rf.log = append(rf.log, &entry)
	index = len(rf.log) - 1
	//DPrintf("%v use Start in index %v and term %v", rf.me, index, term)
	s := ""
	for i, v := range rf.log {
		s = s + strconv.Itoa(i) + ":" + strconv.Itoa(v.Term)
		s += " "
	}
	//DPrintf("%v log is: %v", rf.me, s)
	rf.mu.Unlock()
	return index, term, isLeader
}

// any goroutine with a long-running loop should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// send heartbeat to peers
func (rf *Raft) broadCastAppendEntry() {
	rf.mu.Lock()
	currentTerm := rf.currentTerm
	peersCount := len(rf.peers)
	if rf.state != leader {
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()
	//DPrintf("leader %v start heartbeat\n", rf.me)
	var muTemp sync.Mutex
	cond := sync.NewCond(&muTemp)
	finished := 1
	for i := 0; i < peersCount && rf.killed() == false; i++ {
		if i != rf.me {
			go func(i int, term int) {
				//send heartbeat immediatelly
				rf.mu.Lock()
				entries := make([]*Entry, 0)
				if len(rf.log) > rf.nextIndex[i] {
					entries = append(entries, rf.log[rf.nextIndex[i]:len(rf.log)]...)
				}
				appendEntryArgs := AppendEntryArgs{
					Term:         term,
					LeaderId:     rf.me,
					Entries:      entries,
					PrevLogIndex: rf.nextIndex[i] - 1,
					LeaderCommit: rf.commitIndex,
				}
				if appendEntryArgs.PrevLogIndex >= 0 {
					appendEntryArgs.PrevLogTerm = rf.log[appendEntryArgs.PrevLogIndex].Term
				}
				rf.mu.Unlock()
				appendEntryReply := AppendEntryReply{}
				////DPrintf("%v send heart beat to %v\n", rf.me, i)
				ok := rf.sendAppendEntry(i, &appendEntryArgs, &appendEntryReply)
				muTemp.Lock()
				defer muTemp.Unlock()
				if !ok {
					finished++
					cond.Broadcast()
					return
				}
				if appendEntryReply.Term > term {
					rf.mu.Lock()
					//DPrintf("%v become fellower becase recive big term hb\n", rf.me)
					rf.state = fellower
					rf.votedFor = -1
					rf.currentTerm = appendEntryReply.Term
					rf.lastTimeStamp = time.Now()
					rf.mu.Unlock()
				} else {
					rf.mu.Lock()
					if rf.state == leader {
						if appendEntryReply.Success {
							rf.nextIndex[i] += len(appendEntryArgs.Entries)
							rf.matchIndex[i] = appendEntryArgs.PrevLogIndex + len(appendEntryArgs.Entries)
						} else {
							if appendEntryReply.XTerm == -99 {
								rf.nextIndex[i] = appendEntryArgs.PrevLogIndex + 1 - appendEntryReply.XLen
							} else {
								j := appendEntryArgs.PrevLogIndex - 1
								for ; j >= appendEntryReply.XIndex; j-- {
									if rf.log[j].Term == appendEntryReply.XTerm {
										break
									}
								}
								rf.nextIndex[i] = j + 1
							}
							//DPrintf("%v append entry fail next %v is %v", rf.me, i, rf.nextIndex[i])
							//rf.nextIndex[i]--
						}
					}
					rf.mu.Unlock()
				}
				finished++
				cond.Broadcast()
			}(i, currentTerm)
		}
	}
	muTemp.Lock()
	for finished < peersCount {
		if finished > peersCount/2 {
			// update leader commitIndex
			rf.mu.Lock()
			if rf.state != leader {
				rf.mu.Unlock()
				return
			}
			for i := rf.commitIndex + 1; i < len(rf.log); i++ {
				if rf.log[i].Term != rf.currentTerm {
					continue
				}
				count := 1
				for j := 0; j < peersCount; j++ {
					if j != rf.me && rf.matchIndex[j] >= i {
						count++
					}
					if count > peersCount/2 {
						rf.commitIndex = i
						rf.applyCond.Broadcast()
						break
					}
				}
			}
			rf.mu.Unlock()
		}
		cond.Wait()
	}
	muTemp.Unlock()
}

// start election
func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.currentTerm++
	//DPrintf("%v start election in term %v\n", rf.me, rf.currentTerm)
	peersCount := len(rf.peers)
	rf.state = candidate
	rf.lastTimeStamp = time.Now()
	rf.votedFor = rf.me
	currentTerm := rf.currentTerm
	rf.mu.Unlock()
	voteCount := 1
	voteFinished := 1
	var muTemp sync.Mutex
	cond := sync.NewCond(&muTemp)
	// candidate request vote
	for i := 0; i < peersCount && rf.killed() == false; i++ {
		if i != rf.me {
			go func(i int, term int) {
				voteArgs := RequestVoteArgs{
					Term:         term,
					CandidatedId: rf.me,
					LastLogIndex: len(rf.log) - 1,
				}
				if voteArgs.LastLogIndex >= 0 {
					voteArgs.LastLogTerm = rf.log[voteArgs.LastLogIndex].Term
				} else {
					voteArgs.LastLogTerm = -1
				}
				voteReply := RequestVoteReply{}
				////DPrintf("%v send request vote to %v\n", rf.me, i)
				ok := rf.sendRequestVote(i, &voteArgs, &voteReply)
				muTemp.Lock()
				defer muTemp.Unlock()
				if !ok {
					voteFinished++
					cond.Broadcast()
					return
				}
				if voteReply.Term > currentTerm {
					rf.mu.Lock()
					rf.state = fellower
					rf.currentTerm = voteReply.Term
					rf.votedFor = -1
					rf.lastTimeStamp = time.Now()
					//DPrintf("fail in %v reply bacause term %v bigger than me\n", i, voteReply.Term)
					rf.mu.Unlock()
				} else {
					if voteReply.VoteGranted {
						voteCount++
					}
				}
				voteFinished++
				cond.Broadcast()
			}(i, currentTerm)
		}
	}
	//check vote
	muTemp.Lock()
	for voteCount <= peersCount/2 && voteFinished != peersCount {
		cond.Wait()
	}
	rf.mu.Lock()
	if voteCount > peersCount/2 {
		//be a new leader
		if rf.state != candidate || currentTerm != rf.currentTerm {
			rf.mu.Unlock()
			return
		}
		//DPrintf("%v become new leader in term %v\n", rf.me, rf.currentTerm)
		rf.state = leader
		for i := 0; i < peersCount; i++ {
			rf.nextIndex[i] = len(rf.log)
			rf.matchIndex[i] = 0
		}
		//send heartbeat immediately
		rf.mu.Unlock()
		go rf.broadCastAppendEntry()
		//DPrintf("%v finish election become leader", rf.me)
	} else {
		//fail to be new leader
		if rf.state == candidate {
			rf.state = fellower
			rf.votedFor = -1
			//DPrintf("%v fail in leader election in term %v\n", rf.me, rf.currentTerm)

		}
		rf.mu.Unlock()
	}
	////DPrintf("%v finish election for all", rf.me)
	muTemp.Unlock()
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		rf.mu.Lock()
		state := rf.state
		now := time.Now()
		elapses := now.Sub(rf.lastTimeStamp)
		timeout := rf.elctionTime
		rf.mu.Unlock()
		if state != leader {
			if elapses >= timeout {
				rf.SetElectionTime()
				go rf.startElection()
			}
		} else {
			time.Sleep(200 * time.Millisecond)
			go rf.broadCastAppendEntry()
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for rf.killed() == false {
		if rf.lastApplied < rf.commitIndex {
			//DPrintf("%v apply entry index is %v", rf.me, rf.lastApplied+1)
			// apply the log
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied+1].Command,
				CommandIndex: rf.lastApplied + 1,
			}
			rf.applyChan <- applyMsg
			rf.lastApplied++
		} else {
			rf.applyCond.Wait()
		}
	}
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:       peers,
		persister:   persister,
		me:          me,
		votedFor:    -1,
		state:       fellower,
		currentTerm: 0,
		applyChan:   applyCh,
		commitIndex: 0,
		lastApplied: 0,
		nextIndex:   make([]int, len(peers)),
		matchIndex:  make([]int, len(peers)),
	}
	rf.log = append(rf.log, &Entry{Term: 0})
	rf.applyCond = sync.NewCond(&rf.mu)
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.SetElectionTime()
	rf.lastTimeStamp = time.Now()
	go rf.ticker()
	go rf.applier()
	return rf
}
