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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.824/src/labgob"
	"6.824/src/labrpc"
)

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//

const (
	MIN_TIMEOUT        = 150 * time.Millisecond
	MAX_TIMEOUT        = 300 * time.Millisecond
	HEARTBEAT_INTERVAL = 50 * time.Millisecond
)

type ServerRole int

const (
	Follower  ServerRole = 0
	Candidate ServerRole = 1
	Leader    ServerRole = 2
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

type RaftPersistentState struct {
	CurrentTerm int
	VotedFor    int
	Logs        []LogEntry
}

type RaftVolatileState struct {
	CommitIndex int
	LastApplied int
	Role        ServerRole
	LeaderId    int

	// Leader-specific volatile state
	NextIndex  []int
	MatchIndex []int
}

type RaftState struct {
	PersistentState RaftPersistentState
	VolatileState   RaftVolatileState
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	state     *RaftState          // Raft State
	applyCh   chan ApplyMsg       // channel to send ApplyMsg to the service

	// store some timeout channel
	timer *time.Timer
}

func (rf *Raft) resetTimer() {
	// calc a random delay in MIN_TIMEOUT and MAX_TIMEOUT interval
	delay := MIN_TIMEOUT + time.Duration(rand.Intn(int(MAX_TIMEOUT-MIN_TIMEOUT)))

	// if timer is nil, create a new timer
	if rf.timer == nil {
		rf.timer = time.NewTimer(delay)
		return
	}

	// drain the timer channel
	if !rf.timer.Stop() {
		select {
		case <-rf.timer.C:
		default:
		}
	}

	rf.timer.Reset(delay)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.state.PersistentState.CurrentTerm, rf.state.VolatileState.Role == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	buff := new(bytes.Buffer)
	e := labgob.NewEncoder(buff)
	e.Encode(rf.state.PersistentState)
	rf.persister.SaveRaftState(buff.Bytes())
}

// restore previously persisted state.
// or init with default state if no persisted state
func (rf *Raft) initState(data []byte) {
	// Zero State
	state := &RaftState{
		PersistentState: RaftPersistentState{
			CurrentTerm: 0,
			VotedFor:    -1,
			Logs: []LogEntry{
				{Index: 0, Term: 0, Command: nil},
			},
		},
		VolatileState: RaftVolatileState{
			CommitIndex: 0,
			LastApplied: 0,
			Role:        Follower,
			NextIndex:   nil,
			MatchIndex:  nil,
		},
	}

	// Check if we have persisted state
	if data != nil && len(data) > 0 {
		buff := bytes.NewBuffer(data)
		dec := labgob.NewDecoder(buff)

		var saved RaftPersistentState

		// Voting will occur from term 1; So no point in restoring saved state if term is 0
		if dec.Decode(&saved) == nil && saved.CurrentTerm > 0 {
			state.PersistentState = saved
		}
	}

	rf.state = state
}

// RequestVote RPC arguments structure.
type RequestVoteArgs struct {
	Term         int // candidate's term
	CandidateId  int // candidate's ID
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

// RequestVote RPC reply structure.
type RequestVoteReply struct {
	Term        int  // requested servers term for current server to update its term
	VoteGranted bool // requested servers grant vote or not
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// request Vote RPC implementation
	rf.mu.Lock()
	defer rf.mu.Unlock()

	currentTerm := rf.state.PersistentState.CurrentTerm
	votedFor := rf.state.PersistentState.VotedFor

	logIdx := len(rf.state.PersistentState.Logs) - 1
	logTerm := rf.state.PersistentState.Logs[logIdx].Term

	// current tern is greater
	if currentTerm > args.Term {
		reply.VoteGranted = false
		reply.Term = currentTerm
		return
	}

	// if term is newer
	if currentTerm < args.Term {
		rf.state.PersistentState.CurrentTerm = args.Term
		rf.state.PersistentState.VotedFor = -1
		rf.state.VolatileState.Role = Follower

		currentTerm = args.Term
		votedFor = -1
	}

	// if term is same and vote is already granted
	if currentTerm == args.Term && votedFor != -1 && votedFor != args.CandidateId {
		reply.VoteGranted = false
		reply.Term = currentTerm
		return
	}

	// if log is not up to date
	if logTerm > args.LastLogTerm || (logTerm == args.LastLogTerm && logIdx > args.LastLogIndex) {
		reply.VoteGranted = false
		reply.Term = currentTerm
		return
	}

	// Grant Vote
	rf.state.PersistentState.VotedFor = args.CandidateId
	rf.state.VolatileState.Role = Follower

	reply.VoteGranted = true
	reply.Term = args.Term

	rf.persist()
	rf.resetTimer()
}

// code to send a RequestVote RPC to a server.
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

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

func (rf *Raft) notifyUpStreamApp(prevCommitIndex, newCommitIndex int) {
	for i := prevCommitIndex + 1; i <= newCommitIndex; i++ {
		if rf.applyCh != nil {
			rf.applyCh <- ApplyMsg{
				Command:      rf.state.PersistentState.Logs[i].Command,
				CommandValid: true,
				CommandIndex: i,
			}
		}
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	currentTerm := rf.state.PersistentState.CurrentTerm
	lastLogIdx := len(rf.state.PersistentState.Logs) - 1

	// current tern is greater
	if currentTerm > args.Term {
		reply.Term = currentTerm
		reply.Success = false
		return
	}

	// if term is newer
	if currentTerm < args.Term {
		rf.state.PersistentState.CurrentTerm = args.Term
		rf.state.PersistentState.VotedFor = -1
	}

	if lastLogIdx < args.PrevLogIndex {
		reply.Term = rf.state.PersistentState.CurrentTerm
		reply.Success = false
		reply.ConflictIndex = len(rf.state.PersistentState.Logs)
		reply.ConflictTerm = -1
		return
	}

	if rf.state.PersistentState.Logs[args.PrevLogIndex].Term != args.PrevLogTerm {

		// find first log with same term as prevLogTerm
		for i := args.PrevLogIndex - 1; i >= 0; i-- {
			if rf.state.PersistentState.Logs[i].Term != rf.state.PersistentState.Logs[args.PrevLogIndex].Term {
				reply.ConflictIndex = i + 1
				reply.ConflictTerm = rf.state.PersistentState.Logs[args.PrevLogIndex].Term
				reply.Success = false
				return
			}
		}

		reply.Term = rf.state.PersistentState.CurrentTerm
		reply.ConflictIndex = args.PrevLogIndex
		reply.ConflictTerm = rf.state.PersistentState.Logs[args.PrevLogIndex].Term
		reply.Success = false
		return
	}

	// add entires
	// If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that follow it (§5.3)
	insertIdx := args.PrevLogIndex + 1
	for i, entry := range args.Entries {
		logIdx := insertIdx + i
		if logIdx < len(rf.state.PersistentState.Logs) {
			if rf.state.PersistentState.Logs[logIdx].Term != entry.Term {
				// Conflict! Truncate from here and append the rest
				rf.state.PersistentState.Logs = append(
					rf.state.PersistentState.Logs[:logIdx],
					args.Entries[i:]...,
				)
				break
			}
			// else: entry matches, keep going
		} else {
			// We've reached the end of our log, append all remaining
			rf.state.PersistentState.Logs = append(
				rf.state.PersistentState.Logs,
				args.Entries[i:]...,
			)
			break
		}
	}

	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.state.VolatileState.CommitIndex {
		newCommitIndex := args.LeaderCommit
		if newCommitIndex > len(rf.state.PersistentState.Logs)-1 {
			newCommitIndex = len(rf.state.PersistentState.Logs) - 1
		}

		rf.notifyUpStreamApp(rf.state.VolatileState.CommitIndex, newCommitIndex)
		rf.state.VolatileState.CommitIndex = newCommitIndex
	}

	rf.state.VolatileState.Role = Follower

	reply.Success = true
	reply.Term = rf.state.PersistentState.CurrentTerm

	rf.persist()
	rf.resetTimer()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state.VolatileState.Role != Leader {
		return -1, -1, false
	}

	idx := len(rf.state.PersistentState.Logs)
	entry := LogEntry{
		Command: command,
		Term:    rf.state.PersistentState.CurrentTerm,
		Index:   idx,
	}

	// push back
	rf.state.PersistentState.Logs = append(rf.state.PersistentState.Logs, entry)
	rf.persist()

	return idx, rf.state.PersistentState.CurrentTerm, true
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

func (rf *Raft) updateCommitIndex() {
	logLen := len(rf.state.PersistentState.Logs)

	for i := logLen - 1; i > rf.state.VolatileState.CommitIndex; i-- {
		if rf.state.PersistentState.Logs[i].Term == rf.state.PersistentState.CurrentTerm {
			count := 1
			for j := 0; j < len(rf.state.VolatileState.MatchIndex); j++ {
				if j != rf.me && rf.state.VolatileState.MatchIndex[j] >= i {
					count++
				}
			}

			if count > len(rf.peers)/2 {
				rf.notifyUpStreamApp(rf.state.VolatileState.CommitIndex, i)
				rf.state.VolatileState.CommitIndex = i
				break
			}
		}
	}
}

func (rf *Raft) replicateToPeer(peerIdx int) {
	rf.mu.Lock()

	currTerm := rf.state.PersistentState.CurrentTerm
	commitIndex := rf.state.VolatileState.CommitIndex

	prevLogIndex := rf.state.VolatileState.NextIndex[peerIdx] - 1
	prevLogTerm := rf.state.PersistentState.Logs[prevLogIndex].Term
	entries := append([]LogEntry(nil), rf.state.PersistentState.Logs[rf.state.VolatileState.NextIndex[peerIdx]:]...)

	rf.mu.Unlock()

	args := &AppendEntriesArgs{
		Term:         currTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: commitIndex,
	}

	reply := &AppendEntriesReply{}

	if rf.sendAppendEntries(peerIdx, args, reply) {
		rf.mu.Lock()
		// Another leader is elected
		if reply.Term > currTerm {
			if rf.state.VolatileState.Role == Leader && reply.Term > rf.state.PersistentState.CurrentTerm {
				rf.state.VolatileState.Role = Follower
				rf.state.PersistentState.CurrentTerm = reply.Term
				rf.state.PersistentState.VotedFor = -1
				rf.resetTimer()
			}
		} else if reply.Success == false {
			// decrement nextIndex
			if rf.state.VolatileState.Role == Leader {
				if reply.ConflictTerm == -1 {
					rf.state.VolatileState.NextIndex[peerIdx] = reply.ConflictIndex
				} else {
					// Search our log for ConflictTerm
					// If we have it, set NextIndex to the last entry of that term + 1
					// If we don't have it, set NextIndex to ConflictIndex
					found := false
					for i := len(rf.state.PersistentState.Logs) - 1; i >= 0; i-- {
						if rf.state.PersistentState.Logs[i].Term == reply.ConflictTerm {
							rf.state.VolatileState.NextIndex[peerIdx] = i + 1
							found = true
							break
						}
					}
					if !found {
						rf.state.VolatileState.NextIndex[peerIdx] = reply.ConflictIndex
					}
				}

			}
		} else if reply.Success && reply.Term == currTerm {
			// update nextIndex and matchIndex
			if rf.state.VolatileState.Role == Leader {

				newMatch := prevLogIndex + len(entries)
				if newMatch > rf.state.VolatileState.MatchIndex[peerIdx] {
					rf.state.VolatileState.MatchIndex[peerIdx] = newMatch
				}

				rf.state.VolatileState.NextIndex[peerIdx] = rf.state.VolatileState.MatchIndex[peerIdx] + 1

				rf.updateCommitIndex()
			}
		}

		rf.mu.Unlock()
	}
}

func (rf *Raft) broadcastAppendEntries() {
	for peerIdx := range rf.peers {
		if peerIdx != rf.me {
			go rf.replicateToPeer(peerIdx)
		}
	}
}

func (rf *Raft) startLeaderLoop() {

	rf.broadcastAppendEntries()

	tickCh := time.NewTicker(HEARTBEAT_INTERVAL)
	defer tickCh.Stop()

	for !rf.killed() {
		<-tickCh.C // wait for the tick

		rf.mu.Lock()
		if rf.state.VolatileState.Role != Leader {
			rf.mu.Unlock()
			return
		}

		rf.mu.Unlock()
		rf.broadcastAppendEntries()
	}
}

func (rf *Raft) initLeaderState() {
	rf.state.VolatileState.Role = Leader
	rf.state.VolatileState.LeaderId = rf.me

	rf.state.VolatileState.NextIndex = make([]int, len(rf.peers))
	rf.state.VolatileState.MatchIndex = make([]int, len(rf.peers))

	logLen := len(rf.state.PersistentState.Logs)

	for i := range rf.peers {
		rf.state.VolatileState.NextIndex[i] = logLen
		rf.state.VolatileState.MatchIndex[i] = 0
	}

	rf.state.VolatileState.MatchIndex[rf.me] = logLen - 1
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.resetTimer() // reset timer for election timeout

	rf.state.VolatileState.Role = Candidate

	rf.state.PersistentState.CurrentTerm++
	rf.state.PersistentState.VotedFor = rf.me

	currTerm := rf.state.PersistentState.CurrentTerm
	LastLogIndex := len(rf.state.PersistentState.Logs) - 1
	LastLogTerm := rf.state.PersistentState.Logs[LastLogIndex].Term

	rf.persist()
	rf.mu.Unlock()

	var votes int32 = 1 // vote for self

	args := RequestVoteArgs{
		Term:         currTerm,
		CandidateId:  rf.me,
		LastLogIndex: LastLogIndex,
		LastLogTerm:  LastLogTerm,
	}

	for peerIdx := range rf.peers {
		if peerIdx != rf.me {
			go func(idx int) {

				reply := &RequestVoteReply{}
				if rf.sendRequestVote(idx, &args, reply) {
					// Another leader is already present
					if reply.Term > currTerm {
						rf.mu.Lock()

						if rf.state.VolatileState.Role == Candidate && reply.Term > rf.state.PersistentState.CurrentTerm {
							rf.state.VolatileState.Role = Follower
							rf.state.PersistentState.CurrentTerm = reply.Term
							rf.state.PersistentState.VotedFor = -1
							rf.resetTimer()
						}

						rf.mu.Unlock()

						return
					} else if reply.VoteGranted {
						atomic.AddInt32(&votes, 1)

						if atomic.LoadInt32(&votes) > int32(len(rf.peers)/2) {
							rf.mu.Lock()
							isLeader := false

							// We now have majority votes
							// We need to check if we're still a candidate and in the same term
							// Since in current server multiple elections can take place
							if rf.state.VolatileState.Role == Candidate && rf.state.PersistentState.CurrentTerm == currTerm {
								isLeader = true
								rf.initLeaderState()
							}

							rf.mu.Unlock()

							// start the leader process
							if isLeader {
								go rf.startLeaderLoop()
							}

							return
						}
					}
				}

			}(peerIdx)
		}
	}
}

// timeoutLoop is a background goroutine that handles timeout events.
func (rf *Raft) timeoutLoop() {
	for rf.killed() == false {
		<-rf.timer.C

		rf.mu.Lock()
		role := rf.state.VolatileState.Role
		rf.mu.Unlock()

		if role != Leader {
			go rf.startElection()
		}
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	// initialize state from persister or default state if no persisted state
	rf.initState(persister.ReadRaftState())
	rf.resetTimer()

	go rf.timeoutLoop()

	return rf
}
