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

	"6.824/labgob"
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
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

type Status int

const (
	Follower = iota
	Candidate
	Leader
)

type Log struct {
	Term    int
	Command interface{}
}

func (l Log) String() string {
	return fmt.Sprintf("term: %v, cmd: %v", l.Term, l.Command)
}

const broadcastTime = 40

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm    int
	votedFor       int
	status         Status
	logs           []*Log
	matchedIndexes []int
	nextIndexes    []int
	applyCh        chan ApplyMsg
	commitIndex    int
	lastApplied    int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	isleader = rf.status == Leader
	term = rf.currentTerm
	rf.mu.Unlock()
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	enc := labgob.NewEncoder(w)
	enc.Encode(rf.currentTerm)
	enc.Encode(rf.votedFor)
	enc.Encode(rf.logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		rf.logs = make([]*Log, 1)
		rf.logs[0] = &Log{
			Term:    0,
			Command: nil,
		} //Placeholder, since the index starts from 1
		rf.votedFor = -1
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	dec := labgob.NewDecoder(r)
	var ct int
	if e1 := dec.Decode(&ct); e1 != nil {
		log.Fatalf("Server %v failed to decode currentTerm when restarting: %v", rf.me, e1)
	} else {
		rf.currentTerm = 0
	}
	rf.currentTerm = ct
	var vf int
	if e2 := dec.Decode(&vf); e2 != nil {
		log.Fatalf("Server %v failed to decode votedFor when restarting: %v", rf.me, e2)
	} else {
		rf.votedFor = -1
	}
	rf.votedFor = vf
	var logs []*Log
	if e3 := dec.Decode(&logs); e3 != nil {
		log.Fatalf("Server %v failed to decode logs when restarting: %v", rf.me, e3)
	} else {
		rf.logs = make([]*Log, 1)
		rf.logs[0] = &Log{
			Term:    0,
			Command: nil,
		} //Placeholder, since the index starts from 1
	}
	rf.logs = logs
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
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

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term              int
	CandidateId       int
	LastAppendedIndex int
	LastAppendedTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		return
	}
	defer rf.persist()
	if args.Term > rf.currentTerm {
		rf.stepDown(args.Term)
	}
	reply.Term = rf.currentTerm
	if rf.votedFor != -1 {
		// Server has already voted
		return
	}
	if rf.logs[len(rf.logs)-1].Term > args.LastAppendedTerm || (rf.logs[len(rf.logs)-1].Term == args.LastAppendedTerm && len(rf.logs) > args.LastAppendedIndex+1) {
		// Leader completion property not satisfied
		return
	}
	rf.votedFor = args.CandidateId // Leader completion satisfied, vote for it
	reply.VoteGranted = true
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	Logs         []*Log
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term             int
	Success          bool
	ConflictLogIndex int
	ConflictLogTerm  int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		return
	}
	if rf.currentTerm < args.Term {
		rf.stepDown(args.Term)
	}
	reply.Term = rf.currentTerm
	defer rf.persist()

	rf.status = Follower // Must convert to follower when receiving a heartbeat or log append
	rf.votedFor = args.LeaderId

	// Consistency check:
	// It is not satisfied when:
	// 1: leader's log is longer than the current follower's
	// 2: the logs under the specified index are not the same
	if len(rf.logs) < args.PrevLogIndex+1 {
		reply.ConflictLogIndex = len(rf.logs)
		return
	}
	if rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		for i, v := range rf.logs {
			if v.Term == rf.logs[args.PrevLogIndex].Term {
				reply.ConflictLogIndex = i
				reply.ConflictLogTerm = v.Term
				break
			}
		}
		return
	}

	// If an existing entry conflicts with a new one, delete it and all that follow it, and append all new entries not already in the log
	i, j := args.PrevLogIndex+1, 0
	for ; i+j < len(rf.logs) && j < len(args.Logs); j++ {
		if rf.logs[i+j].Term != args.Logs[j].Term {
			break
		}
	}
	rf.logs = append(rf.logs[:i+j], args.Logs[j:]...)
	//DPrintf("Server %v at term %v: args: %v", rf.me, rf.currentTerm, args)
	//DPrintf("Server %v at term %v: logs: %v", rf.me, rf.currentTerm, rf.logs)

	// Update commit index
	if args.LeaderCommit < len(rf.logs) {
		rf.commitIndex = args.LeaderCommit
	} else {
		rf.commitIndex = len(rf.logs) - 1
	}

	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.status != Leader {
		return 0, rf.currentTerm, false
	}
	newLog := &Log{Term: rf.currentTerm, Command: command}
	rf.logs = append(rf.logs, newLog)
	//DPrintf("Leader %v at term %v : logs: %v", rf.me, rf.currentTerm, rf.logs)
	index := rf.nextIndexes[rf.me]
	rf.nextIndexes[rf.me]++
	rf.persist()
	return index, rf.currentTerm, true
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		rf.mu.Lock()
		if rf.status == Follower {
			// Attempt to be a candidate
			rf.status = Candidate
		}
		rf.mu.Unlock()

		tms := rand.Intn(300) + 150
		time.Sleep(time.Duration(tms) * time.Millisecond)

		// If not receiving heartbeat or vote, start the election
		rf.mu.Lock()
		if rf.status != Candidate {
			rf.mu.Unlock()
			continue
		}
		rf.currentTerm += 1
		rf.votedFor = rf.me
		rf.persist()
		rf.mu.Unlock()

		vote := 1
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			go func(index int) {
				rf.mu.Lock()
				if rf.status != Candidate {
					rf.mu.Unlock()
					return
				}
				args := &RequestVoteArgs{
					Term:        rf.currentTerm,
					CandidateId: rf.me,
				}
				args.LastAppendedIndex = len(rf.logs) - 1
				args.LastAppendedTerm = rf.logs[args.LastAppendedIndex].Term
				rf.mu.Unlock()

				reply := &RequestVoteReply{}
				ok := rf.sendRequestVote(index, args, reply)

				if !ok {
					return
				}

				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.Term < rf.currentTerm {
					return
				}
				if reply.Term > rf.currentTerm {
					rf.stepDown(reply.Term)
					return
				}
				if rf.status != Candidate {
					return
				}

				if reply.VoteGranted {
					vote += 1
				}

				if vote > len(rf.peers)/2 {
					rf.status = Leader
					//DPrintf("Candidate %v is elected at term %v\n", rf.me, rf.currentTerm)
					rf.nextIndexes = make([]int, len(rf.peers))
					rf.matchedIndexes = make([]int, len(rf.peers))
					for i := 0; i < len(rf.peers); i++ {
						rf.nextIndexes[i] = len(rf.logs)
						rf.matchedIndexes[i] = 0
					}
					rf.commitIndex = 0
				}
			}(i)
		}
	}
}

func (rf *Raft) heartbeat() {
	for !rf.killed() {
		for i := range rf.peers {
			if i == rf.me {
				rf.applyLogs()
				continue
			}
			go rf.sendHeartBeat(i)
		}
		time.Sleep(time.Duration(broadcastTime) * time.Millisecond)
	}
}

func (rf *Raft) applyLogs() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for i := rf.lastApplied; i < rf.commitIndex; i++ {
		msg := ApplyMsg{
			CommandValid:  true,
			Command:       rf.logs[i+1].Command,
			CommandIndex:  i + 1,
			SnapshotValid: false,
			Snapshot:      nil,
			SnapshotTerm:  0,
			SnapshotIndex: 0,
		}
		rf.applyCh <- msg
		rf.lastApplied += 1
	}
}

func (rf *Raft) sendHeartBeat(index int) {
	rf.mu.Lock()
	if rf.status != Leader { // Safeguard, should not send heartbeat when the current server is not a leader
		rf.mu.Unlock()
		return
	}
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		LeaderCommit: rf.commitIndex,
	}
	args.PrevLogIndex = rf.nextIndexes[index] - 1
	args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
	args.Logs = make([]*Log, 0)
	args.Logs = append(args.Logs, rf.logs[args.PrevLogIndex+1:]...)
	rf.mu.Unlock()

	reply := &AppendEntriesReply{}
	ok := rf.sendAppendEntries(index, args, reply)

	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term < rf.currentTerm {
		return
	}
	if reply.Term > rf.currentTerm {
		rf.stepDown(reply.Term)
		return
	}
	if rf.status != Leader {
		return
	}

	if !reply.Success {
		conflictTermFound := false
		// If the leader contains the logs of the conflict term, then we should move the cursor back to the last committed log of that term
		if reply.ConflictLogTerm > 0 {
			for i := len(rf.logs) - 1; i > 0; i-- {
				if rf.logs[i].Term == reply.ConflictLogTerm {
					rf.nextIndexes[index] = i + 1
					conflictTermFound = true
					break
				}
			}
		}
		// Follower's logs are much shorter than the leader's
		// Or if no conflict term is found, then set to the conflict log index
		if !conflictTermFound && reply.ConflictLogIndex > 0 { // we assume that the log's index starts from 1
			rf.nextIndexes[index] = reply.ConflictLogIndex
		}

		if rf.nextIndexes[index] > 1 {
			rf.nextIndexes[index]--
		}
	} else {
		matchedIndex := args.PrevLogIndex + len(args.Logs)
		rf.matchedIndexes[index] = matchedIndex
		rf.nextIndexes[index] = matchedIndex + 1

		// Update the commit index when:
		// 1. the log has been replicated in the majority of servers
		// 2. the log is appended at the current term (the leader can only commit the logs at current term)
		vote := 1
		for _, n := range rf.matchedIndexes {
			if n > rf.commitIndex && rf.logs[n].Term == rf.currentTerm {
				vote++
			}
			if vote > len(rf.peers)/2 {
				rf.commitIndex = n
				break
			}
		}
	}
}

func (rf *Raft) stepDown(term int) {
	rf.currentTerm = term
	rf.votedFor = -1
	rf.status = Follower
	rf.persist()
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.applyCh = applyCh
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	// start heartbeat goroutine
	go rf.heartbeat()

	return rf
}
