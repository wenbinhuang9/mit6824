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
	//"bytes"
	"sync"
	"sync/atomic"

	//"6.824/labgob"
	"6.824/labrpc"
	"time"
	"math/rand"
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

const (
	FOLLOWER = 1
	CANDIDATE = 2
	LEADER = 3
	MAXELECTIMEOUT = 1000
	MINELECTIMEOUT = 600 
	NILVOTE = -1
	DEBUG = 1
)

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

	//Persistent state on all servers
	currentTerm int
	votedFor int 
 
	log []Entry
	role int 
	candidateId int 
	
	voteCount map[int]int  

	//Volatile state on all servers:
	commitIndex int 
	lastApplied int 

	//Volatile state on leaders
	nextIndex []int 
	matchIndex []int 
	
	resetTimeoutEvent int64
	applyCh chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term int
	var isleader bool
	// Your code here (2A).

//	rf.mu.Lock()
	term = rf.currentTerm
	isleader = (rf.role == LEADER)
//	rf.mu.Unlock()

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
	Term int 
	CandidateId int;
	LastLogIndex int; 
	LastLogTerm int;  
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//

type Entry struct {
	Term int 
	Command interface{}
	Index int 
}
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term int 
	LeaderId int 
	PrevLogIndex int 
	PrevLogTerm int 
	Entries []Entry
	LeaderCommit int 
}

type AppendEntriesReply struct {
	Term int 
	Success bool
}

func (rf *Raft) matchedPrevLog(argsPrevLogIndex int, argsPrevLogTerm int) bool {
	if argsPrevLogIndex < len(rf.log) && argsPrevLogIndex >= 0  {
		return rf.log[argsPrevLogIndex].Term == argsPrevLogTerm
	}else {
		return false 
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if len(args.Entries) > 0 {
		PrefixDPrintf(rf, "AppendEntries args=%v, reply=%v\n", args, reply)
	}
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	currentTerm := rf.currentTerm
	rf.resetTimeoutEvent = makeTimestamp()

	reply.Term = currentTerm
	// Reply false if term < currentTerm 
	if args.Term < currentTerm {
		reply.Success = false
		reply.Term = currentTerm 
		return
	}
	//If RPC request or response contains term T > currentTerm:
	//set currentTerm = T, convert to follower
	if (args.Term > currentTerm) {
		rf.currentTerm = reply.Term
		rf.votedFor = NILVOTE
		if rf.role == LEADER  {
			DPrintf("LeaderCondition sorry server %d  term %d not a leader, logs %v, commitIndex %d\n",rf.me, rf.currentTerm, rf.log, rf.commitIndex) 
		}
		rf.role = FOLLOWER

	}

	//discover leader, convert to follower
	rf.role = FOLLOWER
	
	// Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm
	if rf.matchedPrevLog(args.PrevLogIndex, args.PrevLogTerm) == false {
		
		// PrefixDPrintf(rf, "unmatched prev log index %v\n", args)

		reply.Success = false 
		reply.Term = currentTerm
		return 
	}
	//If an existing entry conflicts with a new one (same index
	//but different terms), delete the existing entry and all that
	//follow it
	//todo not a efficient way, but simple
	if len(args.Entries) > 0 {
		rf.log = rf.log[0 : (args.PrevLogIndex + 1)]

		// Append any new entries not already in the log
		rf.log = append(rf.log, args.Entries...)	
	}

	//. If leaderCommit > commitIndex, set commitIndex =
    //min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		oriCommitIdx := rf.commitIndex

		rf.commitIndex = MinOf(rf.log[len(rf.log) - 1].Index, args.LeaderCommit)
		PrefixDPrintf(rf, "update commitIndex to %d\n", rf.commitIndex)
		for N := oriCommitIdx + 1; N <= rf.commitIndex; N += 1 {
			command := ApplyMsg{}
			command.CommandValid = true 
			command.CommandIndex = N
			command.Command = rf.log[N].Command
			rf.applyCh <- command
		} 
	}

	reply.Term = rf.currentTerm
	reply.Success = true

	if len(args.Entries) > 0 {
		PrefixDPrintf(rf, "AppendEntries Success %v\n", rf)
	}	
}

func MinOf(vars ...int) int {
    min := vars[0]

    for _, i := range vars {
        if min > i {
            min = i
        }
    }

    return min
}

func (rf *Raft) sendHeartbeats() {
	for rf.killed() == false {
		time.Sleep(50 * time.Millisecond)

		//The tester requires that the leader send heartbeat RPCs no more than ten times per second.
		if (rf.role == LEADER) {
			n := len(rf.peers)
			for server := 0; server < n; server+=1 {
				if server != rf.me {
					go rf.sendOneHeartbeat(server)	
				}
			}
		}
	}
}

func (rf *Raft) sendOneHeartbeat(server int) bool{
	rf.mu.Lock()
	if rf.role != LEADER {
		rf.mu.Unlock()
		return false
	}
	rf.mu.Unlock()

	args := rf.makeAppendEntriesArgs(server)
	reply := AppendEntriesReply{}
	return rf.sendAppendEntries(server, &args, &reply)
}

func (rf *Raft) sendAppendEntries(server int , args *AppendEntriesArgs, reply *AppendEntriesReply) bool{
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
//
// example RequestVote RPC handler.
//


func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	currentTerm := rf.currentTerm

	//If RPC request or response contains term T > currentTerm:
	//set currentTerm = T, convert to follower
	if (args.Term > currentTerm) {
		rf.currentTerm = args.Term
		rf.votedFor = NILVOTE

		if rf.role == LEADER  {
			DPrintf("LeaderCondition sorry server %d  term %d not a leader, logs %v, commitIndex %d\n",rf.me, rf.currentTerm, rf.log, rf.commitIndex) 
		} 
		rf.role = FOLLOWER
	}

	if args.Term < currentTerm {
		// Reply false if term < currentTerm 
		reply.VoteGranted = false
		reply.Term = currentTerm 
	}else {
		//If votedFor is null or candidateId,
		//and candidate’s log is at least as up-to-date as receiver’s log,
		//&& rf.atLeastUptodate(args.LastLogIndex, args.LastLogTerm)
		if (rf.votedFor == NILVOTE || rf.votedFor == args.CandidateId) &&  rf.atLeastUptodate(args.LastLogIndex, args.LastLogTerm) {
			rf.votedFor = args.CandidateId	
			reply.VoteGranted = true
			reply.Term = rf.currentTerm
		}else {
			reply.VoteGranted = false
			reply.Term = rf.currentTerm
		}	
	}
	rf.mu.Unlock()
}

//Raft determines which of two logs is more up-to-date
//by comparing the index and term of the last entries in the
//logs. 
//If the logs have last entries with different terms, then
//the log with the later term is more up-to-date.
// If the logs end with the same term, then whichever log is longer is more up-to-date.
func (rf *Raft) atLeastUptodate(candidateLastLogIdx int , candidateLastLogTerm int) bool{
	revLastLogIdx, revLastLogTerm := rf.lastLogIdxAndTerm()

	if candidateLastLogTerm > revLastLogTerm {
		return true
	}else if candidateLastLogTerm < revLastLogTerm {

		return false
	}else{
		//candidateLastLogTerm == revLastLogTerm
		return candidateLastLogIdx >= revLastLogIdx
	}
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
func makeEntry(command interface{}, term int , index int ) Entry{
	return Entry{term, command, index}
}
func (rf *Raft) Start(command interface{}) (int, int, bool) {

	index := -1
	term := -1
	isLeader := true
	// Your code here (2B).
	// The leader appends the command to its log as a new entry
	// AppendEntries RPCs in parallel to each of the other servers to replicate the entry
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader = (rf.role == LEADER) 

	if isLeader == false {
		return index, term, isLeader 
	}

	PrefixDPrintf(rf, "Start args %v\n", command)
	// The leader appends the command to its log as a new entry 
	index = len(rf.log)
	term = rf.currentTerm
	entry := makeEntry(command, rf.currentTerm, index)
	rf.log = append(rf.log, entry)
	rf.nextIndex[rf.me] = len(rf.log)
	rf.matchIndex[rf.me] = len(rf.log) - 1

	// AppendEntries RPCs in parallel to each of the other servers to replicate the entry
	for peer, _ := range rf.peers {
		if peer != rf.me {
			lastLogIdx := len(rf.log) - 1
			peerNextIdx := rf.nextIndex[peer]
			if lastLogIdx >= peerNextIdx {
				rf.sendAppendEntriesAsync(peer)
			}	
		}
	}
	PrefixDPrintf(rf, "Start reply %d, %d, %t | %v\n", index, term, isLeader, rf.log)
	return index, term, isLeader
}

func (rf *Raft) makeAppendEntriesArgs(peer int) AppendEntriesArgs {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		var args AppendEntriesArgs 
		lastLogIdx := len(rf.log) - 1
		peerNextIdx := rf.nextIndex[peer]

		if lastLogIdx >= peerNextIdx {
			args.Entries = rf.log[peerNextIdx : len(rf.log)]
		}else {
			args.Entries = []Entry{}
		}

		args.LeaderId = rf.me 
		args.PrevLogIndex = peerNextIdx - 1
		if args.PrevLogIndex >= len(rf.log) {
			PrefixDPrintf(rf, "current peer %d, and raft %v\n", peer, rf)
		}
		args.PrevLogTerm = rf.log[args.PrevLogIndex].Term 
		args.Term = rf.currentTerm
		args.LeaderCommit = rf.commitIndex

		return args 
}

func (rf *Raft) sendAppendEntriesAsync(peer int) {	

	go func() {
		rf.mu.Lock()

		if rf.role != LEADER {
			PrefixDPrintf(rf, "not a leader, stop to send AppendEntries")
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

		args := rf.makeAppendEntriesArgs(peer)
		var reply AppendEntriesReply

		ok := rf.sendAppendEntries(peer, &args, &reply)

		rf.sendAppendEntriesCallback(ok , peer, args, reply)
	}() 
}

func (rf *Raft) retryAppendEntriesASync(peer int) {

	time.Sleep(10 * time.Millisecond)

	rf.sendAppendEntriesAsync(peer)

}
func (rf *Raft) sendAppendEntriesCallback(ok bool, peer int, args AppendEntriesArgs, reply AppendEntriesReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	PrefixDPrintf(rf, "AppendEntries Callback From peer %d, reply %v", peer, reply)
	if rf.role != LEADER{
		return 
	}
	if ok == false {
		//retry 
		rf.retryAppendEntriesASync(peer)
		return 
	}
	//If RPC request or response contains term T > currentTerm:
	//set currentTerm = T, convert to follower (§5.1)
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term 
		rf.votedFor = NILVOTE
		if rf.role == LEADER  {
			DPrintf("LeaderCondition sorry server %d  term %d not a leader, logs %v, commitIndex %d\n",rf.me, rf.currentTerm, rf.log, rf.commitIndex) 
		}	
		rf.role = FOLLOWER
		return 
	}

	if reply.Success == true {
		//If successful: update nextIndex and matchIndex for follower (5.3)
		if (len(args.Entries) > 0) {
			argsLastEntry := args.Entries[len(args.Entries) - 1]
			newNextIdx := argsLastEntry.Index + 1
	
			// if ( newNextIdx >= len(rf.log)) {
			// 	//todo the same request may return multiple success 
			// 	newNextIdx = len(rf.log)
			// 	PrefixDPrintf(rf, "next index %d bigger than log length %d, %v", newNextIdx, len(rf.log), args.Entries)
			// }
			rf.nextIndex[peer] = newNextIdx
	
			newMatchedIdx := newNextIdx - 1
			rf.matchIndex[peer] = newMatchedIdx
		}
	}else {
		//If AppendEntries fails because of log inconsistency 
		// decrement nextIndex and retry
		rf.nextIndex[peer] = rf.nextIndex[peer] - 1
		rf.retryAppendEntriesASync(peer) 
	}

	go rf.updateCommitIndex()
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

func makeTimestamp() int64 {
    return time.Now().UnixNano() / int64(time.Millisecond)
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.

func electionTimeout() int64 {
	return int64(rand.Intn(MAXELECTIMEOUT- MINELECTIMEOUT) + MINELECTIMEOUT)
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		rf.mu.Lock()
		isLeader := (rf.role == LEADER)
		if isLeader{
			//doing nothing 
		}else {
			current := makeTimestamp()
			elaspedTime := current - rf.resetTimeoutEvent 
			electTimeOut := electionTimeout()
			if elaspedTime >= electTimeOut {
				
				go rf.startElection()	
			}
		}
		rf.mu.Unlock()

		time.Sleep(10 * time.Millisecond)
	}
}
func (rf *Raft) sendRequestVoteAsync(server int, args RequestVoteArgs, reply RequestVoteReply) {
	go func(){
		ok := rf.sendRequestVote(server, &args, &reply)
		rf.sendRequestVoteCallBack(ok, server, args, reply )
	}()

}

// todo 
func (rf *Raft) sendRequestVoteCallBack(ok bool, server int, args RequestVoteArgs, reply RequestVoteReply) {
	if ok == false {
		PrefixDPrintf(rf, "sendRequestVote to peer %d fails\n", server)
		return 
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if(rf.role != CANDIDATE) {
		return
	}
	
	//If RPC request or response contains term T > currentTerm:
	//set currentTerm = T, convert to follower

	if reply.Term > rf.currentTerm {
		PrefixDPrintf(rf, "stops election because of smaller currenterm = %d, reply term =%d, reply server = %d\n", rf.currentTerm, reply.Term, server)
		rf.role = FOLLOWER
		rf.currentTerm = reply.Term
		rf.votedFor = NILVOTE
		// rf.voteCount = make(map[int]int)
		return
		 
	} else if rf.role == CANDIDATE && reply.VoteGranted == true && reply.Term == rf.currentTerm  {
		PrefixDPrintf(rf, "got voted from server %d\n", server)
		rf.voteCount[server] = 1
		DPrintf("server %d get vote nums= %d\n", rf.me, len(rf.voteCount))
	}	

	if (len(rf.voteCount) > len(rf.peers) / 2 && rf.role == CANDIDATE) {
		rf.role = LEADER
		DPrintf("LeaderCondition congratulations server %d  term %d becomes leader, got total votes %d, logs %v, commitIndex %d\n", 
		rf.me, rf.currentTerm, len(rf.voteCount),rf.log, rf.commitIndex)

		for i, _ := range rf.nextIndex {
			rf.nextIndex[i] = len(rf.log)
		}
		for i, _ := range rf.matchIndex {
			rf.matchIndex[i] = 0 
		}

		n := len(rf.peers)
		//send initial empty AppendEntries RPCs (heartbeat) to each server in async
		for server := 0; server < n; server += 1{
			if (server != rf.me) {
				go rf.sendOneHeartbeat(server)
			}
		}
	}
}

func (rf *Raft) updateCommitIndex() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if  rf.role != LEADER {
		return 
	}
	//If there exists an N such that N > commitIndex, a majority
	//of matchIndex[i] ≥ N, and log[N].term == currentTerm:
	//set commitIndex = N 

	// binary search is ok 
	// for simplicity, use linear search here 
	var N int 

	for N = len(rf.log) - 1; N > rf.commitIndex && rf.log[N].Term == rf.currentTerm; N -= 1 {
		cnt := 0
		for i, _ := range rf.matchIndex {
			if rf.matchIndex[i] >= N {
				cnt += 1
			}
		}
		// a majority of matchIndex[i] ≥ N,
		if cnt > len(rf.peers) / 2 {
			oriCommitIdx := rf.commitIndex
			rf.commitIndex = N 
			PrefixDPrintf(rf, "commitIndex updates to %d\n", N)

			for cidx := oriCommitIdx + 1; cidx <= rf.commitIndex; cidx+=1 {
				command := ApplyMsg{}
				command.CommandValid = true 
				command.CommandIndex = cidx
				command.Command = rf.log[cidx].Command
				rf.applyCh <- command
			}
			return
		}
	}
}
func (rf *Raft) startElection() {
	
	DPrintf("server %d start election\n", rf.me)

	rf.mu.Lock()

	rf.role = CANDIDATE
	rf.currentTerm += 1 
	rf.voteCount = make(map[int]int)
	rf.votedFor = rf.candidateId

	rf.voteCount[rf.me] = 1

	rf.resetTimeoutEvent = makeTimestamp()
	rf.mu.Unlock()

	//Send RequestVote RPCs to all other servers 
	n := len(rf.peers)
	for server := 0; server < n; server += 1 {
		if server != rf.me {
			// todo make it concurrent here
			lastLogIdx, lastLogTerm := rf.lastLogIdxAndTerm() 

			args := RequestVoteArgs {rf.currentTerm, rf.candidateId, lastLogIdx, lastLogTerm}
			var  reply  RequestVoteReply
			rf.sendRequestVoteAsync(server, args, reply) // any return value here 
		}
	} 
}

func (rf *Raft) lastLogIdxAndTerm() (int, int ) {
	var revLastLogIdx int 
	var revLastLogTerm int 

	lastEntry := rf.log[len(rf.log) - 1]
	revLastLogIdx = lastEntry.Index 
	revLastLogTerm = lastEntry.Term 


	return revLastLogIdx, revLastLogTerm
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
	rf.candidateId = me
	rf.votedFor = NILVOTE 
	rf.role = FOLLOWER
	rf.currentTerm = 0
	rf.resetTimeoutEvent = makeTimestamp()
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	entry := Entry {0, nil, 0}
	rf.log = append(rf.log, entry)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	// have the leader send heartbeats out periodically
	go rf.sendHeartbeats()
	return rf
}
