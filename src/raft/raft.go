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

	"fmt"
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
	currentTerm int
	votedFor    int
	status      Status
	logs        []*Log
	//matchedIndexes []int
	nextIndexes []int
	applyCh     chan ApplyMsg
	commitIndex int
	lastApplied int
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
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
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

func (rf *Raft) TermUpdate(otherTerm int) (bool, int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if otherTerm < rf.currentTerm {
		return false, rf.currentTerm
	}
	if otherTerm > rf.currentTerm {
		rf.currentTerm = otherTerm
		rf.status = Follower
	}
	return true, rf.currentTerm
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	term, _ := rf.GetState()
	if term >= args.Term {
		// Stale request or the current server has already voted
		reply.VoteGranted = false
		reply.Term = term
		return
	}
	_, term = rf.TermUpdate(args.Term)

	if rf.logs[len(rf.logs)-1].Term > args.LastAppendedTerm || (rf.logs[len(rf.logs)-1].Term == args.LastAppendedTerm && len(rf.logs) > args.LastAppendedIndex+1) {
		// Leader completion property not satisfied
		reply.VoteGranted = false
		reply.Term = term
		return
	}
	rf.mu.Lock()
	rf.votedFor = args.CandidateId // Leader completion satisfied, vote for it
	rf.mu.Unlock()

	reply.VoteGranted = true
	reply.Term = term
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
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	DPrintf("Server %v: received AppendEntries %v", rf.me, args)
	isValid, term := rf.TermUpdate(args.Term)
	if !isValid {
		// Stale request
		reply.Success = false
		reply.Term = term
		//DPrintf("Server %v: received stale request: %+v", rf.me, args)
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock() //TODO: make sure the lock is necessary
	rf.status = Follower // TODO: this is a bit messy with the TermUpdate method
	rf.votedFor = args.LeaderId

	// Consistency check:
	// It is not satisfied when:
	// 1: leader's log is longer than the current follower's
	// 2: the logs under the specified index are not the same
	if args.PrevLogIndex > len(rf.logs)-1 {
		//DPrintf("Server %v: consistency check failed: %+v", rf.me, args)
		reply.Success = false
		reply.Term = args.Term
		return
	}
	if rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		//DPrintf("Server %v: log consistency check failed: %+v", rf.me, args)
		reply.Success = false
		reply.Term = args.Term
		// Remove the existing entry and all following that
		rf.logs = rf.logs[:args.PrevLogIndex]
		return
	}

	// Append/overwrite the logs, if the logs have not been applied (idempotent application)
	if len(args.Logs) > 0 {
		//DPrintf("Server %v: received AppendEntries request: %v", rf.me, args)
		for i := 0; i < len(args.Logs); i++ {
			currentIndex := args.PrevLogIndex + i + 1
			if currentIndex > len(rf.logs)-1 {
				rf.logs = append(rf.logs, args.Logs[i])
			} else if args.Logs[i].Term != rf.logs[currentIndex].Term && args.Logs[i].Command != rf.logs[currentIndex].Command {
				rf.logs[currentIndex] = args.Logs[i]
			}
		}
		DPrintf("Server %v: logs: %v", rf.me, rf.logs)
		//freshLogs := rf.logs[args.PrevLogIndex+1:]
		//if !isSubset(args.Logs, freshLogs) {
		//	rf.logs = append(rf.logs[:args.PrevLogIndex+1], args.Logs...)
		//	DPrintf("Server %v: logs: %v", rf.me, rf.logs)
		//}
	}
	if args.LeaderCommit < len(rf.logs) {
		rf.commitIndex = args.LeaderCommit
	} else {
		//TODO: not sure
		rf.commitIndex = len(rf.logs) - 1
	}
	for i := rf.lastApplied; i < rf.commitIndex; i++ {
		DPrintf("Server %v: sending apply msg at index %v with args %v", rf.me, i+1, args)
		rf.applyCh <- ApplyMsg{
			CommandValid:  true,
			Command:       rf.logs[i+1].Command,
			CommandIndex:  i + 1,
			SnapshotValid: false,
			Snapshot:      nil,
			SnapshotTerm:  0,
			SnapshotIndex: 0,
		}
		rf.lastApplied++
	}
	reply.Success = true
	reply.Term = term
}

func isSubset(l1 []*Log, l2 []*Log) bool {
	if len(l1) <= len(l2) {
		for i := 0; i < len(l1); i++ {
			if l1[i].Term == l2[i].Term && l1[i].Command == l2[i].Command {
				continue
			}
			return false
		}
		return true
	}
	return false
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
	term, isLeader := rf.GetState()
	if isLeader == false {
		return 0, term, false
	}
	// Your code here (2B).
	rf.mu.Lock()
	log := &Log{Term: term, Command: command}
	rf.logs = append(rf.logs, log)
	DPrintf("Leader %v at term %v : logs: %v", rf.me, term, rf.logs)
	index := rf.nextIndexes[rf.me]
	rf.nextIndexes[rf.me]++
	rf.mu.Unlock()

	replicated := 1

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(replica int, t int) {
			//incrementNextIndex := false
			rf.mu.Lock()
			matchedIndex := rf.nextIndexes[replica] - 1
			rf.nextIndexes[replica]++
			rf.mu.Unlock()
			for rf.killed() == false {
				rf.mu.Lock()
				prevLogIndex := matchedIndex
				//prevLogIndex := rf.nextIndexes[replica] - 1
				//if !incrementNextIndex {
				//	rf.nextIndexes[replica]++
				//	incrementNextIndex = true
				//}
				request := &AppendEntriesArgs{
					Term:         t,
					LeaderId:     rf.me,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  rf.logs[prevLogIndex].Term,
					Logs:         rf.logs[prevLogIndex+1:],
					//LeaderCommit: leaderCommit,	//TODO should we bring the latest?
				}
				rf.mu.Unlock()
				reply := &AppendEntriesReply{}
				ok := rf.sendAppendEntries(replica, request, reply)
				if !ok {
					// Retry until the request succeeds
					time.Sleep(time.Duration(10) * time.Millisecond)
					continue
				}
				if isValid, _ := rf.TermUpdate(reply.Term); !isValid {
					return
				}
				if reply.Term > t {
					//TODO: if the term is older?
					return
				}
				if !reply.Success {
					//DPrintf("Replica %v failed the consistency check %v", replicaIndex, request)
					// Consistency check failed, so decrease the nextIndex, and retry
					rf.mu.Lock()
					//if rf.nextIndexes[replica] > 1 {
					//	rf.nextIndexes[replica]--
					//}
					if matchedIndex > 1 {
						matchedIndex--
					}
					rf.mu.Unlock()
					time.Sleep(time.Duration(10) * time.Millisecond)
					continue
				}
				// Replication on followers succeeds
				rf.mu.Lock()
				//newNextIndex := prevLogIndex + len(request.Logs) + 1
				//if newNextIndex > rf.nextIndexes[replica] {
				//	rf.nextIndexes[replica] = newNextIndex
				//}
				//if prevLogIndex+len(request.Logs) > rf.matchedIndexes[replica] {
				//	rf.matchedIndexes[replica] = prevLogIndex + len(request.Logs)
				//}
				replicated += 1
				if replicated == len(rf.peers)/2+1 && prevLogIndex+len(request.Logs) > rf.commitIndex {
					rf.commitIndex = prevLogIndex + len(request.Logs)
				}
				rf.mu.Unlock()
				break
			}
		}(i, term)
	}

	return index, term, true
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
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		rf.mu.Lock()
		if rf.status == Follower {
			// Attempt to be a candidate
			rf.status = Candidate
		}
		rf.mu.Unlock()

		tms := rand.Intn(150) + 150
		time.Sleep(time.Duration(tms) * time.Millisecond)

		// If not receiving heartbeat or vote, start the election
		rf.mu.Lock()
		if rf.status != Candidate {
			rf.mu.Unlock()
			continue
		}
		rf.currentTerm += 1
		rf.votedFor = rf.me
		currentTerm := rf.currentTerm
		//fmt.Printf("After %v ms, candidate %v starts election at term %v \n", tms, rf.me, currentTerm)
		rf.mu.Unlock()

		// Parallely send RequestVote rpc call to other peers
		vote := 1
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			rf.mu.Lock()
			lastAppendedIndex := len(rf.logs) - 1
			lastAppendedTerm := rf.logs[lastAppendedIndex].Term
			rf.mu.Unlock()

			go func(index int) {
				args := &RequestVoteArgs{
					Term:              currentTerm,
					CandidateId:       rf.me,
					LastAppendedIndex: lastAppendedIndex,
					LastAppendedTerm:  lastAppendedTerm,
				}

				reply := &RequestVoteReply{}
				rf.sendRequestVote(index, args, reply)

				if isValid, _ := rf.TermUpdate(reply.Term); !isValid {
					return
				}
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if rf.status != Candidate {
					return
				}

				if reply.VoteGranted {
					vote += 1
				}

				if vote > len(rf.peers)/2 {
					rf.status = Leader
					DPrintf("Candidate %v is elected at term %v\n", rf.me, rf.currentTerm)
					// Initialize the nextIndexes array
					rf.nextIndexes = make([]int, len(rf.peers))
					//rf.matchedIndexes = make([]int, len(rf.peers))
					for i := 0; i < len(rf.peers); i++ {
						rf.nextIndexes[i] = len(rf.logs)
						//rf.matchedIndexes[i] = 0
					}
				}

			}(i)
		}
	}
}

func (rf *Raft) heartbeat() {
	for rf.killed() == false {
		term, isLeader := rf.GetState()
		if isLeader {
			for i := range rf.peers {
				if i == rf.me {
					// Apply msgs if the lastApplied is behind
					for i := rf.lastApplied; i < rf.commitIndex; i++ {
						rf.applyCh <- ApplyMsg{
							CommandValid:  true,
							Command:       rf.logs[i+1].Command,
							CommandIndex:  i + 1,
							SnapshotValid: false,
							Snapshot:      nil,
							SnapshotTerm:  0,
							SnapshotIndex: 0,
						}
						rf.lastApplied += 1
					}
					continue
				}
				go func(index int) {
					args := &AppendEntriesArgs{
						Term:         term,
						LeaderId:     rf.me,
						LeaderCommit: rf.commitIndex,
						PrevLogIndex: rf.nextIndexes[index] - 1,
						PrevLogTerm:  rf.logs[rf.nextIndexes[index]-1].Term,
					}

					reply := &AppendEntriesReply{}
					DPrintf("Leader %v sent heartbeat to server %v: %v", rf.me, index, args)
					ok := rf.sendAppendEntries(index, args, reply)

					rf.mu.Lock()
					defer rf.mu.Unlock()

					// TODO: can be simplified
					if !ok || reply.Term < rf.currentTerm {
						return
					}
					if reply.Term > rf.currentTerm || !reply.Success {
						rf.currentTerm = reply.Term
						rf.status = Follower
					}
				}(i)
			}
		}
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
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
	rf.logs = make([]*Log, 1)
	rf.logs[0] = &Log{
		Term:    0,
		Command: nil,
	} //Placeholder, since the index starts from 1
	rf.applyCh = applyCh
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	// start heartbeat goroutine
	go rf.heartbeat()

	return rf
}
