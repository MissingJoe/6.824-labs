package raft

import (
	"bytes"

	"sync"
	"sync/atomic"

	"6.824/labgob"

	"math/rand"
	"time"

	"strconv"

	"6.824/labrpc"
)

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
	mu                sync.Mutex          // Lock to protect shared access to this peer's state
	peers             []*labrpc.ClientEnd // RPC end points of all peers
	persister         *Persister          // Object to hold this peer's persisted state
	me                int                 // this peer's index into peers[]
	dead              int32               // set by Kill()
	state             int
	currentTerm       int
	votedFor          int
	log               []*Entry
	applyChan         chan ApplyMsg
	bufferApplyChan   chan ApplyMsg
	applyCond         *sync.Cond
	elctionTime       time.Duration
	lastTimeStamp     time.Time
	commitIndex       int
	lastApplied       int
	nextIndex         []int
	matchIndex        []int
	snapshot          []byte
	lastIncludedIndex int
	lastIncludedTerm  int
}

const (
	fellower  = 0
	candidate = 1
	leader    = 2
)

func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
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

func (rf *Raft) getRaftState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	return w.Bytes()
}

// save Raft's persistent state to stable storage,
func (rf *Raft) persist() {
	rf.persister.SaveRaftState(rf.getRaftState())
	s := ""
	for i, v := range rf.log {
		s = s + strconv.Itoa(i+rf.lastIncludedIndex) + ":" + strconv.Itoa(v.Term)
		s += " "
	}
	DPrintf("%v persist currentTerm %v votedFor %v lastIncludeIndex %v log:%v", rf.me, rf.currentTerm, rf.votedFor, rf.lastIncludedIndex, s)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var lastIncludedIndex int
	var log []*Entry
	if d.Decode(&currentTerm) != nil {
		DPrintf("%v error in decode current term", rf.me)
	} else if d.Decode(&votedFor) != nil {
		DPrintf("%v error in decode votefor", rf.me)
	} else if d.Decode(&log) != nil {
		DPrintf("%v error in decode log", rf.me)
	} else if d.Decode(&lastIncludedIndex) != nil {
		DPrintf("%v error in decode lastIncludeIndex", rf.me)
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.lastIncludedIndex = lastIncludedIndex
	}
	s := ""
	for i, v := range rf.log {
		s = s + strconv.Itoa(i+rf.lastIncludedIndex) + ":" + strconv.Itoa(v.Term)
		s += " "
	}
	DPrintf("%v read persist currentTerm %v votedFor %v lastIncludeIndex %v log:%v", rf.me, rf.currentTerm, rf.votedFor, rf.lastIncludedIndex, s)
}

func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.commitIndex >= lastIncludedIndex || lastIncludedIndex <= rf.lastIncludedIndex {
		DPrintf("%v snapshot index %v term %v is out of fashion", rf.me, lastIncludedIndex, lastIncludedTerm)
		return false
	}
	log := make([]*Entry, 0)
	log = append(log, &Entry{Term: lastIncludedTerm})
	if rf.lastIncludedIndex+len(rf.log) > lastIncludedIndex {
		log = append(log, rf.log[lastIncludedIndex-rf.lastIncludedIndex+1:]...)
	}
	rf.log = log
	rf.snapshot = snapshot
	rf.commitIndex = lastIncludedIndex
	rf.lastApplied = lastIncludedIndex
	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm
	rf.persister.SaveStateAndSnapshot(rf.getRaftState(), snapshot)
	DPrintf("%v install snapshot with snapIndex %v snapTerm %v", rf.me, rf.lastIncludedIndex, rf.lastIncludedTerm)
	s := ""
	for i, v := range rf.log {
		s = s + strconv.Itoa(i+rf.lastIncludedIndex) + ":" + strconv.Itoa(v.Term)
		s += " "
	}
	DPrintf("%v log:%v", rf.me, s)
	return true
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.lastIncludedIndex {
		return
	}
	rf.snapshot = snapshot
	log := make([]*Entry, len(rf.log)+rf.lastIncludedIndex-index)
	copy(log, rf.log[index-rf.lastIncludedIndex:])
	rf.log = log
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = log[0].Term
	rf.persister.SaveStateAndSnapshot(rf.getRaftState(), snapshot)
	DPrintf("%v create snapshot with snapIndex %v Term %v", rf.me, index, rf.lastIncludedTerm)
	s := ""
	for i, v := range rf.log {
		s = s + strconv.Itoa(i+rf.lastIncludedIndex) + ":" + strconv.Itoa(v.Term)
		s += " "
	}
	DPrintf("%v log:%v", rf.me, s)
}

func (rf *Raft) SetElectionTime() {
	rand.Seed(time.Now().UnixNano())
	rf.elctionTime = time.Duration(rand.Intn(200)+300) * time.Millisecond
	DPrintf("%v election time is %v\n", rf.me, rf.elctionTime)
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Offset            int
	Data              []byte
	Done              bool
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		DPrintf("%v term %v is smaller than my term %v\n", rf.me, args.Term, rf.currentTerm)
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		if rf.state != fellower {
			DPrintf("%v is recive big term so become fellower\n", rf.me)
			rf.state = fellower
		}
		rf.votedFor = -1
		DPrintf("%v modify currentTerm\n", rf.me)
		rf.persist()
	}
	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		DPrintf("%v snapshot from %v is out of fashion", rf.me, args.LeaderId)
		return
	}
	applyMsg := ApplyMsg{
		CommandValid:  false,
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
	rf.bufferApplyChan <- applyMsg
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	if !ok {
		return false
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		DPrintf("%v become fellower becase recive big term hb\n", rf.me)
		rf.state = fellower
		rf.votedFor = -1
		rf.currentTerm = reply.Term
		DPrintf("%v modify currentTerm and voteFor\n", rf.me)
		rf.persist()
		return false
	}
	rf.nextIndex[server] = args.LastIncludedIndex + 1
	rf.matchIndex[server] = args.LastIncludedIndex
	DPrintf("%v snapshot install rpc to %v successfully snapindex %v", rf.me, server, args.LastIncludedIndex)
	return ok
}

type RequestVoteArgs struct {
	Term         int
	CandidatedId int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.VoteGranted = false
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		DPrintf("%v term %v is smaller than my term %v\n", rf.me, args.Term, rf.currentTerm)
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		if rf.state != fellower {
			DPrintf("%v is recive big term so become fellower\n", rf.me)
			rf.state = fellower
		}
		rf.votedFor = -1
		DPrintf("%v modify currentTerm\n", rf.me)
		rf.persist()
	}
	if rf.votedFor == -1 || rf.votedFor == args.CandidatedId {
		lastLogIndex := len(rf.log) - 1
		lastTerm := -1
		if lastLogIndex >= 0 {
			lastTerm = rf.log[lastLogIndex].Term
		}
		if lastTerm < args.LastLogTerm || (lastTerm == args.LastLogTerm && args.LastLogIndex >= lastLogIndex+rf.lastIncludedIndex) {
			reply.VoteGranted = true
			DPrintf("%v vote for %v in term %v\n", rf.me, args.CandidatedId, rf.currentTerm)
			rf.votedFor = args.CandidatedId
			DPrintf("%v modify voteFor\n", rf.me)
			rf.persist()
			rf.lastTimeStamp = time.Now()
		} else {
			DPrintf("%v no vote for %v in term %v for term reason\n", rf.me, args.CandidatedId, rf.currentTerm)
		}
	} else {
		DPrintf("%v already vote for %v and wait for heartbeat\n", rf.me, rf.votedFor)
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

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
	DPrintf("%v recive heartbeat from %v with entry length %v\n", rf.me, args.LeaderId, len(args.Entries))
	if args.Term < rf.currentTerm {
		DPrintf("%v term %v is smaller than my term %v\n", rf.me, args.Term, rf.currentTerm)
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		if rf.state != fellower {
			DPrintf("%v is recive big term so become fellower\n", rf.me)
			rf.state = fellower
		}
		rf.votedFor = -1
		DPrintf("%v modify currentTerm\n", rf.me)
		rf.persist()
	}
	rf.lastTimeStamp = time.Now()

	if args.PrevLogIndex == -1 {
		// fisrt heartbeat
		reply.Success = true
	} else if args.PrevLogIndex-rf.lastIncludedIndex < 0 {
		reply.Success = true
	} else {
		if args.PrevLogIndex-rf.lastIncludedIndex > len(rf.log)-1 {
			DPrintf("%v recive appendentry PrevLogIndex %v is empty\n", rf.me, args.PrevLogIndex)
			reply.XTerm = -99
			reply.XLen = 0
			for i := args.PrevLogIndex - rf.lastIncludedIndex; i >= len(rf.log); i-- {
				reply.XLen++
			}
			DPrintf("%v recive appendentry xlen is %v\n", rf.me, reply.XLen)
		} else {
			if rf.log[args.PrevLogIndex-rf.lastIncludedIndex].Term == args.PrevLogTerm {
				if len(args.Entries) != 0 {
					for i, entry := range args.Entries {
						index := i + args.PrevLogIndex + 1
						if index >= len(rf.log)+rf.lastIncludedIndex {
							rf.log = append(rf.log, entry)
						} else {
							if rf.log[index-rf.lastIncludedIndex].Term != args.Entries[i].Term {
								rf.log = rf.log[:index-rf.lastIncludedIndex]
								rf.log = append(rf.log, entry)
							}
						}
					}
					DPrintf("%v modify log\n", rf.me)
					rf.persist()
				}
				DPrintf("%v match preindex %v and term %v\n", rf.me, args.PrevLogIndex, args.PrevLogTerm)
				reply.Success = true

				//check leadercommit
				if args.LeaderCommit > rf.commitIndex {
					if args.LeaderCommit <= len(rf.log)-1+rf.lastIncludedIndex {
						rf.commitIndex = args.LeaderCommit
					} else {
						rf.commitIndex = len(rf.log) - 1 + rf.lastIncludedIndex
					}
					DPrintf("%v fellower commitIndex %v", rf.me, rf.commitIndex)
					rf.applyCond.Broadcast()
				}
			} else {
				DPrintf("%v don't match preindex %v and term %v\n", rf.me, args.PrevLogIndex, args.PrevLogTerm)
				reply.XTerm = rf.log[args.PrevLogIndex-rf.lastIncludedIndex].Term
				i := args.PrevLogIndex - rf.lastIncludedIndex
				for ; rf.log[i].Term == reply.XTerm && i >= 1; i-- {
				}
				reply.XIndex = i + 1
				DPrintf("%v don't match preindex xterm is %v, xindex is %v\n", rf.me, reply.XTerm, reply.XIndex)
			}
		}
	}
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	return ok
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	rf.mu.Lock()
	if rf.state != leader {
		isLeader = false
		rf.mu.Unlock()
		return index, term, isLeader
	}
	term = rf.currentTerm
	entry := Entry{
		Command: command,
		Term:    term,
	}
	rf.log = append(rf.log, &entry)
	index = len(rf.log) - 1 + rf.lastIncludedIndex
	DPrintf("%v use Start in index %v and term %v", rf.me, index, term)
	DPrintf("%v modify log\n", rf.me)
	rf.persist()
	rf.mu.Unlock()
	go rf.broadCastAppendEntry()
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
	DPrintf("leader %v start heartbeat\n", rf.me)
	var muTemp sync.Mutex
	cond := sync.NewCond(&muTemp)
	finished := 1
	for i := 0; i < peersCount && rf.killed() == false; i++ {
		if i != rf.me {
			go func(i int, term int) {
				rf.mu.Lock()
				if rf.lastIncludedIndex >= rf.nextIndex[i] {
					rf.mu.Unlock()
					DPrintf("%v log in %v is out of fashion installsnapshot of me lastIndex %v", rf.me, i, rf.lastIncludedIndex)
					installSnapshotArgs := InstallSnapshotArgs{
						Term:              term,
						LeaderId:          rf.me,
						LastIncludedIndex: rf.lastIncludedIndex,
						LastIncludedTerm:  rf.lastIncludedTerm,
						Offset:            0,
						Data:              rf.snapshot,
						Done:              true,
					}
					installSnapshotReply := InstallSnapshotReply{}
					go rf.sendInstallSnapshot(i, &installSnapshotArgs, &installSnapshotReply)
					return
				}
				entries := make([]*Entry, 0)
				if len(rf.log) > rf.nextIndex[i]-rf.lastIncludedIndex {
					entries = append(entries, rf.log[rf.nextIndex[i]-rf.lastIncludedIndex:len(rf.log)]...)
				}
				appendEntryArgs := AppendEntryArgs{
					Term:         term,
					LeaderId:     rf.me,
					Entries:      entries,
					PrevLogIndex: rf.nextIndex[i] - 1,
					LeaderCommit: rf.commitIndex,
				}
				if appendEntryArgs.PrevLogIndex-rf.lastIncludedIndex >= 0 &&
					appendEntryArgs.PrevLogIndex-rf.lastIncludedIndex < len(rf.log) {
					appendEntryArgs.PrevLogTerm = rf.log[appendEntryArgs.PrevLogIndex-rf.lastIncludedIndex].Term
				}
				rf.mu.Unlock()
				appendEntryReply := AppendEntryReply{}
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
					DPrintf("%v become fellower becase recive big term hb\n", rf.me)
					rf.state = fellower
					rf.votedFor = -1
					rf.currentTerm = appendEntryReply.Term
					DPrintf("%v modify currentTerm and voteFor\n", rf.me)
					rf.persist()
					rf.mu.Unlock()
				} else {
					rf.mu.Lock()
					if rf.state == leader {
						if appendEntryReply.Success {
							if appendEntryArgs.PrevLogIndex-rf.lastIncludedIndex >= 0 {
								rf.nextIndex[i] += len(appendEntryArgs.Entries)
								rf.matchIndex[i] = appendEntryArgs.PrevLogIndex + len(appendEntryArgs.Entries)
							}
						} else {
							if appendEntryReply.XTerm == -99 {
								rf.nextIndex[i] = appendEntryArgs.PrevLogIndex + 1 - appendEntryReply.XLen
							} else {
								j := appendEntryArgs.PrevLogIndex - 1 - rf.lastIncludedIndex
								for ; j >= appendEntryReply.XIndex; j-- {
									if rf.log[j].Term == appendEntryReply.XTerm {
										break
									}
								}
								rf.nextIndex[i] = j + 1 + rf.lastIncludedIndex
							}
							DPrintf("%v append entry fail next %v is %v", rf.me, i, rf.nextIndex[i])
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
				break
			}
			for i := rf.commitIndex + 1 - rf.lastIncludedIndex; i < len(rf.log); i++ {
				if rf.log[i].Term != rf.currentTerm {
					continue
				}
				count := 1
				for j := 0; j < peersCount; j++ {
					if j != rf.me && rf.matchIndex[j]-rf.lastIncludedIndex >= i {
						count++
					}
					if count > peersCount/2 {
						rf.commitIndex = i + rf.lastIncludedIndex
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
	DPrintf("%v start election in term %v\n", rf.me, rf.currentTerm)
	peersCount := len(rf.peers)
	rf.state = candidate
	rf.lastTimeStamp = time.Now()
	rf.votedFor = rf.me
	currentTerm := rf.currentTerm
	DPrintf("%v modify currentTerm and voteFor\n", rf.me)
	rf.persist()
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
					LastLogIndex: len(rf.log) - 1 + rf.lastIncludedIndex,
				}
				if voteArgs.LastLogIndex >= 0 {
					voteArgs.LastLogTerm = rf.log[voteArgs.LastLogIndex-rf.lastIncludedIndex].Term
				} else {
					voteArgs.LastLogTerm = -1
				}
				voteReply := RequestVoteReply{}
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
					DPrintf("%v modify currentTerm and voteFor\n", rf.me)
					rf.persist()
					DPrintf("fail in %v reply bacause term %v bigger than me\n", i, voteReply.Term)
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
		DPrintf("%v become new leader in term %v\n", rf.me, rf.currentTerm)
		rf.state = leader
		for i := 0; i < peersCount; i++ {
			rf.nextIndex[i] = len(rf.log) + rf.lastIncludedIndex
			rf.matchIndex[i] = 0
		}
		//send heartbeat immediately
		rf.mu.Unlock()
		go rf.broadCastAppendEntry()
		DPrintf("%v finish election become leader", rf.me)
	} else {
		//fail to be new leader
		if rf.state == candidate {
			rf.state = fellower
			rf.votedFor = -1
			DPrintf("%v modify voteFor\n", rf.me)
			rf.persist()
			DPrintf("%v fail in leader election in term %v\n", rf.me, rf.currentTerm)
		}
		rf.mu.Unlock()
	}
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
			time.Sleep(150 * time.Millisecond)
			go rf.broadCastAppendEntry()
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	go func() {
		for msg := range rf.bufferApplyChan {
			rf.applyChan <- msg
		}
	}()
	for rf.killed() == false {
		if rf.lastApplied < rf.commitIndex {
			// apply the log
			rf.lastApplied++
			DPrintf("%v apply entry index is %v", rf.me, rf.lastApplied)
			applyMsg := ApplyMsg{
				CommandValid:  true,
				Command:       rf.log[rf.lastApplied-rf.lastIncludedIndex].Command,
				CommandIndex:  rf.lastApplied,
				SnapshotValid: false,
			}
			rf.bufferApplyChan <- applyMsg
		} else {
			rf.applyCond.Wait()
		}
	}
	close(rf.bufferApplyChan)
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:             peers,
		persister:         persister,
		me:                me,
		votedFor:          -1,
		state:             fellower,
		currentTerm:       0,
		applyChan:         applyCh,
		commitIndex:       0,
		lastApplied:       0,
		nextIndex:         make([]int, len(peers)),
		matchIndex:        make([]int, len(peers)),
		lastIncludedIndex: 0,
		lastIncludedTerm:  0,
		bufferApplyChan:   make(chan ApplyMsg, 20),
	}
	rf.log = append(rf.log, &Entry{Term: 0})
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.readPersist(persister.ReadRaftState())
	rf.lastIncludedTerm = rf.log[0].Term
	rf.snapshot = rf.persister.ReadSnapshot()
	rf.lastApplied = rf.lastIncludedIndex
	rf.commitIndex = rf.lastIncludedIndex
	rf.SetElectionTime()
	rf.lastTimeStamp = time.Now()
	go rf.ticker()
	go rf.applier()
	return rf
}
