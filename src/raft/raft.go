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
	"bytes"
	"sync"
	"sync/atomic"
	"6.824/labgob"
	"6.824/labrpc"
	"time"
	"math/rand"
	"runtime"
	"fmt"
	"strconv"
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
	MAXELECTIMEOUT = 400 //1000
	MINELECTIMEOUT = 250 //600
	HEARTBEATTIMEOUT = 50// 
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r);

	if d.Decode(&rf.currentTerm) != nil ||  d.Decode(&rf.votedFor) != nil || d.Decode(&rf.log) != nil {
		PrefixDPrintf(rf, "decode fails\n");
	}

	PrefixDPrintf(rf, "readPersist curTerm %d, rf.votedFor %d\n", rf.currentTerm, rf.votedFor)
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
	LogId string  
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
	ConflictIndex int 
	ConflictTerm int 
}

func (rf *Raft) matchedPrevLog(argsPrevLogIndex int, argsPrevLogTerm int) bool {
	if argsPrevLogIndex < len(rf.log) && argsPrevLogIndex >= 0  {
		return rf.log[argsPrevLogIndex].Term == argsPrevLogTerm
	}else {
		return false 
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	PrefixDPrintf(rf, "%s start AppendEntries args=%v, currentTerm %d\n",args.LogId, args, rf.currentTerm)
	// if len(args.Entries) > 0 {
	// 	PrefixDPrintf(rf, "%s start AppendEntries args=%v, currentTerm %d\n",args.LogId, args, rf.currentTerm)
	// }
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	currentTerm := rf.currentTerm

	reply.Term = currentTerm
	Assert(args.Term >=0 , "term must >=0")
	// Reply false if term < currentTerm 
	if args.Term < currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm 

		PrefixDPrintf(rf, "%s finished AppendEntries reply %v\n", args.LogId, reply)
		return
	}
	//if the term in the AppendEntries arguments is outdated, you should not reset your timer
	rf.resetTimeoutEvent = makeTimestamp()

	//If RPC request or response contains term T > currentTerm:
	//set currentTerm = T, convert to follower
	if (args.Term > currentTerm) {
		if rf.role == LEADER  {
			DPrintf("LeaderCondition sorry server %d  term %d not a leader, logs %v, commitIndex %d\n",rf.me, rf.currentTerm, rf.log, rf.commitIndex) 
		}
		rf.currentTerm = reply.Term
		rf.votedFor = NILVOTE

		rf.role = FOLLOWER
		rf.persist()
	}

	//discover leader, convert to follower
	rf.role = FOLLOWER
	rf.persist()	
	// Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm
	if rf.matchedPrevLog(args.PrevLogIndex, args.PrevLogTerm) == false {
		
		PrefixDPrintf(rf, "unmatched prev log index %v\n", args)

		reply.Success = false 
		reply.Term = rf.currentTerm
		//If a follower does not have prevLogIndex in its log, 
		//it should return with conflictIndex = len(log) and conflictTerm = None.
		if (args.PrevLogIndex >= len(rf.log)) {
			reply.ConflictIndex = len(rf.log) 
			reply.ConflictTerm = -1 
		}else if (args.PrevLogTerm != rf.log[args.PrevLogIndex].Term) {
			//If a follower does have prevLogIndex in its log, but the term does not match,
			//it should return conflictTerm = log[prevLogIndex].Term, 
			//and then search its log for the first index whose entry has term equal to conflictTerm.	
			reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
			reply.ConflictIndex = args.PrevLogIndex
			i := args.PrevLogIndex - 1 
			for i>= 0 && rf.log[i].Term == reply.ConflictTerm {
				reply.ConflictIndex = i 
				i -= 1 
			}
		}else {
			Assert(true, "Appendentries")
		}
		PrefixDPrintf(rf, "%s finished AppendEntries reply %v\n", args.LogId, reply)
		return 
	}
	
	//todo not a efficient way, but simple
	for _, e := range args.Entries {
		if e.Index < len(rf.log) && e.Term == rf.log[e.Index].Term{
			// do nothing
			Assert(e.Index == rf.log[e.Index].Index, "AppendEntries") 
		}else if e.Index < len(rf.log) && e.Term != rf.log[e.Index].Term {
			//If an existing entry conflicts with a new one (same index
			//but different terms), delete the existing entry and all that
			//follow it
			rf.log = rf.log[0:e.Index]
			rf.log = append(rf.log, e)
			rf.persist()

			Assert(e.Index == rf.log[e.Index].Index, "AppendEntries") 
		}else {
			// Append any new entries not already in the log
			rf.log = append(rf.log, e)
			rf.persist()
		}
	}
	// if len(args.Entries) > 0 {
	// 	rf.log = rf.log[0 : (args.PrevLogIndex + 1)]

		
	// 	rf.log = append(rf.log, args.Entries...)
	// 	rf.persist()	
	// }

	//. If leaderCommit > commitIndex, set commitIndex =
	//min(leaderCommit, index of last new entry)
	// Assert(args.LeaderCommit >= rf.commitIndex, "leader %d, leaderCommit= %d < peer %d, rf.commitIndex= %d", args.LeaderId, args.LeaderCommit, rf.me, rf.commitIndex)
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
			PrefixDPrintf(rf, "peer %d |commitIndex %d |command %d", rf.me, N, rf.log[N].Command)
		} 
	}

	reply.Term = rf.currentTerm
	reply.Success = true

	if len(args.Entries) > 0 {
		PrefixDPrintf(rf, "%s AppendEntries Success %v\n", args.LogId, rf)
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
		time.Sleep(HEARTBEATTIMEOUT * time.Millisecond)

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
	args := rf.makeAppendEntriesArgs(server)

	rf.mu.Unlock()

	reply := AppendEntriesReply{}
	ret := rf.sendAppendEntries(server, &args, &reply)

	//If RPC request or response contains term T > currentTerm:
	//set currentTerm = T, convert to follower (§5.1)
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term 
		rf.votedFor = NILVOTE
		if rf.role == LEADER  {
			DPrintf("LeaderCondition sorry server %d  term %d not a leader, logs %v, commitIndex %d\n",rf.me, rf.currentTerm, rf.log, rf.commitIndex) 
		}	
		rf.role = FOLLOWER
		rf.persist() 
	}

	return ret 
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
	defer rf.mu.Unlock()
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
		rf.persist()
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
			i , t := rf.lastLogIdxAndTerm()
			PrefixDPrintf(rf, "voted to candidate %d, args %v, lastlogIndex %d, lastlogTerm %d\n", args.CandidateId, args, i, t)
			rf.votedFor = args.CandidateId
			rf.persist()	
			reply.VoteGranted = true
			reply.Term = rf.currentTerm
			//you grant a vote to another peer.
			rf.resetTimeoutEvent = makeTimestamp()
		}else {
			reply.VoteGranted = false
			reply.Term = rf.currentTerm
		}	
	}
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
	rf.persist()
	rf.nextIndex[rf.me] = len(rf.log)
	Assert(rf.nextIndex[rf.me] >= 1, "nextindex should be bigger than 1 me:%d", rf.me)
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
	
		var args AppendEntriesArgs 
		lastLogIdx := len(rf.log) - 1
		Assert(lastLogIdx >= 0, "lastLogIndex shoudld >= 1")
		peerNextIdx := rf.nextIndex[peer]

		Assert(peerNextIdx >=1 , "peerNextIdx should >= 1, peer %d, nextIndex: %v", peer, rf.nextIndex)

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
		args.LogId =  strconv.Itoa(rf.me) +  "_" + strconv.FormatInt(time.Now().UnixNano(), 10)  
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
		
		args := rf.makeAppendEntriesArgs(peer)
		rf.mu.Unlock()
		
		var reply AppendEntriesReply

		ok := rf.sendAppendEntries(peer, &args, &reply)
		if ok {
			rf.sendAppendEntriesCallback(ok , peer, args, reply)
		}else {
			PrefixDPrintf(rf, "send fails\n")
		}
	}() 
}

func (rf *Raft) retryAppendEntriesASync(peer int) {

	time.Sleep(20 * time.Millisecond)

	rf.sendAppendEntriesAsync(peer)

}
func (rf *Raft) sendAppendEntriesCallback(ok bool, peer int, args AppendEntriesArgs, reply AppendEntriesReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	PrefixDPrintf(rf, "%s AppendEntries Callback From peer %d, reply %v, args %v\n",args.LogId , peer, reply, args)
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
		rf.persist()
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
			Assert(rf.nextIndex[peer] >= 1, "nextIndex should >= 1, peer %d", peer)

			// newMatchedIdx := newNextIdx - 1
			rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
		}
	}else {
		//If AppendEntries fails because of log inconsistency 
		// decrement nextIndex and retry
		// rf.nextIndex[peer] = rf.nextIndex[peer] - 1
		if reply.ConflictIndex > 0 {
			PrefixDPrintf(rf, "update next index to conflictindex %d\n", reply.ConflictIndex)
			rf.nextIndex[peer] = reply.ConflictIndex
		}else {
			rf.nextIndex[peer] -= 1
		}
		if (rf.nextIndex[peer] <= 0) {
			rf.nextIndex[peer] = 1
		}
		Assert(rf.nextIndex[peer] >= 1, "nextIndex should >= 1, peer %d, args %v, reply %v, peers log leng %d", peer, args, reply, len(rf.log))

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
		rf.persist()
		return
		 
	} else if rf.role == CANDIDATE && reply.VoteGranted == true && reply.Term == rf.currentTerm  {
		PrefixDPrintf(rf, "got voted from server %d\n", server)
		rf.voteCount[server] = 1
		DPrintf("server %d get vote nums= %d\n", rf.me, len(rf.voteCount))
	}	
	count := 0
	for _, v := range rf.voteCount {
		if v == 1 {
			count += 1
		}
	}
	if (count > len(rf.peers) / 2 && rf.role == CANDIDATE) {
		rf.role = LEADER
		DPrintf("LeaderCondition congratulations server %d  term %d becomes leader, got total votes %d, logs %v, commitIndex %d\n", 
		rf.me, rf.currentTerm, count,rf.log, rf.commitIndex)

		for i, _ := range rf.nextIndex {
			rf.nextIndex[i] = len(rf.log)
			Assert(rf.nextIndex[i] >= 1,  "rf.nextIdx should >= 1 , i: %d", i)
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
				PrefixDPrintf(rf, "updateCommitIndex in Leader peer %d|commitIndex %d|command %d", rf.me, cidx, command.Command)
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
	rf.persist()

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
func Assert(flag bool, format string, a ...interface{}) {
    if (!flag) {
        _, _, lineno, ok := runtime.Caller(1);
   
        if (ok) {
            t := time.Now();
            a = append([]interface{} { t.Format("2006-01-02 15:04:05.00"), lineno }, a...);
            reason := fmt.Sprintf("%s [raft:%d] " + format + "\n", a...);
            panic(reason);
        } else {
            panic("");
        }
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
	rf.applyCh = applyCh
	rf.candidateId = me
	rf.votedFor = NILVOTE 
	rf.role = FOLLOWER
	rf.currentTerm = 0
	rf.resetTimeoutEvent = makeTimestamp()
	rf.commitIndex = 0
	rf.lastApplied = 0


	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	if(len(rf.log) == 0) {
		entry := Entry {0, nil, 0}
		rf.log = append(rf.log, entry)
		rf.persist()
	}

	Assert(len(rf.log) >= 1, "raft log not bigger than 0")

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	// start ticker goroutine to start elections
	go rf.ticker()

	// have the leader send heartbeats out periodically
	go rf.sendHeartbeats()
	return rf
}
