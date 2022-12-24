package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"sync"
	"sync/atomic"

	//	"6.824/labgob"

	"math/rand"
	"time"

	"6.824/labrpc"
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

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state         int
	currentTerm   int
	votedFor      int
	log           []interface{}
	applyChan     chan ApplyMsg
	elctionTime   time.Duration
	lastTimeStamp time.Time
}

const (
	fellower  = 0
	candidate = 1
	leader    = 2
)

// return currentTerm and whether this server
// believes it is the leader.
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

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidatedId int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	currentTerm := rf.currentTerm
	rf.mu.Unlock()
	if args.Term < currentTerm {
		reply.VoteGranted = false
		reply.Term = currentTerm
		//DPrintf("term %v is smaller than my term %v\n", args.Term, currentTerm)
		return
	}

	if args.Term > currentTerm {
		rf.mu.Lock()
		rf.currentTerm = args.Term
		if rf.state != fellower {
			//DPrintf("%v is recive big term so become fellower\n", rf.me)
			rf.state = fellower
			rf.votedFor = -1
		}
		rf.mu.Unlock()
	}

	rf.mu.Lock()
	votedFor := rf.votedFor
	if votedFor == -1 || votedFor == args.CandidatedId {
		reply.Term = rf.currentTerm
		rf.votedFor = args.CandidatedId
		//DPrintf("%v vote for %v in term %v\n", rf.me, args.CandidatedId, rf.currentTerm)
		rf.lastTimeStamp = time.Now()
		reply.VoteGranted = true
	} else {
		//DPrintf("%v already vote for %v and wait for heartbeat\n", rf.me, rf.votedFor)
	}
	rf.mu.Unlock()
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// RPC AppendEntry
type AppendEntryArgs struct {
	Term     int
	LeaderId int
	Entries  []interface{}
	//LeaderCommit int
}

type AppendEntryReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	currentTerm := rf.currentTerm
	rf.mu.Unlock()
	if args.Term < currentTerm {
		reply.Success = false
		reply.Term = currentTerm
		//DPrintf("term %v is smaller than my term %v\n", args.Term, currentTerm)
		return
	}

	if args.Term > currentTerm {
		rf.mu.Lock()
		rf.currentTerm = args.Term
		if rf.state != fellower {
			//DPrintf("%v is recive big term so become fellower\n", rf.me)
			rf.state = fellower
			rf.votedFor = -1
		}
		rf.mu.Unlock()
	}

	//if candidate recive AppendEntry
	rf.mu.Lock()
	if rf.state == candidate {
		rf.state = fellower
		rf.votedFor = -1
	}
	rf.mu.Unlock()

	if len(args.Entries) == 0 {
		//DPrintf("%v recive heartbeat from %v\n", rf.me, args.LeaderId)
		//reset voteFor
		rf.mu.Lock()
		rf.lastTimeStamp = time.Now()
		rf.votedFor = -1
		reply.Term = rf.currentTerm
		reply.Success = true
		rf.mu.Unlock()
	} else {
		//append entry
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

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// send heartbeat to peers
func (rf *Raft) broadCastHeartBeat() {
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
				heartBeatArgs := AppendEntryArgs{
					Term:     term,
					LeaderId: rf.me,
					Entries:  make([]interface{}, 0),
				}
				heartBeatReply := AppendEntryReply{}
				////DPrintf("%v send heart beat to %v\n", rf.me, i)
				ok := rf.sendAppendEntry(i, &heartBeatArgs, &heartBeatReply)
				muTemp.Lock()
				defer muTemp.Unlock()
				if !ok {
					finished++
					cond.Broadcast()
					return
				}
				if heartBeatReply.Term > currentTerm {
					//DPrintf("%v become fellower becase recive big term hb\n", rf.me)
					rf.mu.Lock()
					rf.state = fellower
					rf.votedFor = -1
					rf.mu.Unlock()
				} else {
					finished++
					cond.Broadcast()
				}
			}(i, currentTerm)
		}
	}
	muTemp.Lock()
	for finished < peersCount {
		cond.Wait()
	}
	////DPrintf("leader %v finish heartbeat\n", rf.me)
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
					rf.votedFor = -1
					rf.mu.Unlock()
					//DPrintf("fail in %v reply bacause term %v bigger than me\n", i, voteReply.Term)
					return
				} else {
					if voteReply.VoteGranted {
						voteCount++
					}
					voteFinished++
					cond.Broadcast()
				}
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
		rf.votedFor = -1
		//send heartbeat immediately
		rf.mu.Unlock()
		go rf.broadCastHeartBeat()
	} else {
		//fail to be new leader
		//DPrintf("%v fail in leader election in term %v\n", rf.me, rf.currentTerm)
		rf.votedFor = -1
		rf.state = fellower
		rf.mu.Unlock()
	}
	muTemp.Unlock()
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using time.Sleep().
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
			go rf.broadCastHeartBeat()
		}
		time.Sleep(time.Microsecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
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
		elctionTime: 0,
	}

	////DPrintf("%v has been made\n", me)
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// start ticker goroutine to start elections
	rf.SetElectionTime()
	rf.lastTimeStamp = time.Now()
	go rf.ticker()
	return rf
}
