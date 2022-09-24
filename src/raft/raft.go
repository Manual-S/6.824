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
	"context"
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"

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
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

const (
	Leader    = 1
	Follower  = 2
	Candidate = 3
)

var StateMapping = map[int]string{
	Leader:    "leader",
	Follower:  "follower",
	Candidate: "candidate",
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state 锁 并发保护
	peers     []*labrpc.ClientEnd // RPC end points of all peers整个集群信息，集群中的每一台机器是一个peer,它在peers这个数组的下标为me
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int // 服务器当前的任期
	votedFor    int // 当前任期内收到选票的候选人 如果没有投给任何候选人 则为空
	State       int // 当前节点的状态

	electionChan  chan bool
	heartbeatChan chan bool

	electionTimeout   int64 // 发起选举超时时间
	electionDuration  int64 // 超时选举的周期
	heartbeatDuration int64 // leader发送心跳包的周期
	heartbeatTimeout  int64 // 发送心跳包的超时时间

	ctx context.Context
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isLeader bool

	term = rf.currentTerm
	if rf.State == Leader {
		isLeader = true
	}
	return term, isLeader
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
// 恢复之前的一致性状态
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

type AppendEntriesArgs struct {
	LeaderTerm int // leader的任期
	LeaderID   int // leader的id

}

type AppendEntriesReply struct {
	Term       int  // follower的当前任期
	Success    bool // 如果跟随者所含有的条目和 prevLogIndex 以及 prevLogTerm 匹配上了，则为 true
	FollowerID int
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int // 候选人的任期号
	CandidateId int // 候选人的id
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	VoteGranted bool // 当候选人赢得了此张选票时为真
	Term        int  // 当前投票人的任期
	FollowerId  int  // 投票人的id
}

// coverState 改变状态
//func (rf *Raft) coverState(state int64) {
//	switch state {
//	case Follower:
//		rf.votedFor = nil
//		rf.State = Follower
//	case Candidate:
//		rf.State = Candidate
//		rf.currentTerm++
//		rf.votedFor = &rf.me
//		rf.resetTimerElection()
//	case Leader:
//		rf.State = Leader
//		rf.resetTimerHeartbeat()
//	}
//}

// RequestVote follower收到candidate要求投票的rpc
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[RequestVote] %d receive Vote,term = %v can = %d\n can term = %v",
		rf.me, rf.currentTerm, args.CandidateId, args.Term)
	if args.Term < rf.currentTerm {
		// 拒绝投票
		reply.VoteGranted = false
		reply.FollowerId = rf.me
		reply.Term = rf.currentTerm
		return
	}

	if args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		// 当前节点已经投过票 并且投的不是要求投票的候选人
		// 拒绝投票
		reply.VoteGranted = false
		reply.FollowerId = rf.me
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		// 当前节点需要给候选人投票
		rf.currentTerm = args.Term
		rf.State = Follower
	}

	// 可以投票
	rf.votedFor = args.CandidateId
	reply.VoteGranted = true
	reply.Term = rf.currentTerm
	reply.FollowerId = rf.me

	// 重置超时的选举时间
	rf.resetTimerElection()

	DPrintf("[RequestVote] %d receive RequestVote Finish", rf.me)
}

// AppendEntries 接受到AppendEntries后
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("[AppendEntries] %v %v %v %v", rf.me, rf.currentTerm, args.LeaderID, args.LeaderTerm)

	if args.LeaderTerm < rf.currentTerm {
		// 领导人的任期比当前的任期小 返回假
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.FollowerID = rf.me
		return
	}

	if args.LeaderTerm > rf.currentTerm {
		rf.currentTerm = args.LeaderTerm
	}

	rf.State = Follower
	// 重置超时选举时间
	rf.resetTimerElection()

	reply.Success = true
	reply.Term = rf.currentTerm
	reply.FollowerID = rf.me

	return
}

// 发送请求投票信息
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
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

func (rf *Raft) mainLoop() {
	for {
		select {
		case <-rf.electionChan:
			// 开启选举流程
			rf.startElection()
		case <-rf.heartbeatChan:
			// 广播心跳包
			rf.sendHeartbeat()
		}
	}
}

// sendHeartbeat 发送心跳包
func (rf *Raft) sendHeartbeat() {

	rf.mu.Lock()
	if rf.State != Leader {
		rf.mu.Unlock()
		return
	}
	args := AppendEntriesArgs{
		LeaderTerm: rf.currentTerm,
		LeaderID:   rf.me,
	}
	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(id int) {
			reply := AppendEntriesReply{}
			if rf.sendAppendEntries(id, &args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.Term > rf.currentTerm {
					rf.State = Follower
					rf.votedFor = -1
					rf.currentTerm = reply.Term
				}
			}
		}(i)
	}

	rf.resetTimerHeartbeat()
}

// startElection 开始选举流程
func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.State = Candidate
	rf.votedFor = rf.me
	rf.currentTerm++
	votes := 1
	request := RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
	}
	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		// 并行的发送选举rpc
		go func(peer int) {
			reply := RequestVoteReply{}
			if rf.sendRequestVote(peer, &request, &reply) {
				// 加锁
				rf.mu.Lock()
				defer rf.mu.Unlock()
				// 解锁
				if reply.Term > rf.currentTerm {
					rf.State = Follower
					rf.currentTerm = reply.Term
					rf.votedFor = -1
				} else if reply.Term == rf.currentTerm && rf.State == Candidate {
					if reply.VoteGranted {
						votes++
						if votes > len(rf.peers)/2 {
							DPrintf("[startElection] %v is leader term = %v", rf.me, rf.currentTerm)
							// 已经获得全部选票的一半 当选leader
							rf.State = Leader
							// 发送心跳包
							rf.heartbeatChan <- true
						}
					}
				}
			}
		}(i)
	}

	rf.resetTimerElection()
}

// resetTimerElection 重置超时选举时间
func (rf *Raft) resetTimerElection() {
	rand.Seed(time.Now().UnixNano())
	rf.electionDuration = rand.Int63n(150) + rf.heartbeatDuration*5
	rf.electionTimeout = time.Now().UnixMilli() + rf.electionDuration
}

// 超时后向chan中发送消息 然后开始选举
func (rf *Raft) timerElection() {
	for {
		rf.mu.Lock()
		if rf.State != Leader {
			timeNow := time.Now().UnixMilli()
			if timeNow-rf.electionTimeout > 0 {
				rf.electionChan <- true
			}
		}
		rf.mu.Unlock()
		time.Sleep(time.Millisecond * 10)
	}
}

// 超时后向chan中发送消息 然后发送心跳包
func (rf *Raft) timerHeartbeat() {
	for {
		rf.mu.Lock()
		if time.Now().UnixMilli()-rf.heartbeatTimeout > 0 {
			rf.heartbeatChan <- true
		}
		rf.mu.Unlock()
		time.Sleep(time.Millisecond * 10)
	}

}

// resetTimerHeartbeat 重置心跳包
func (rf *Raft) resetTimerHeartbeat() {
	rf.heartbeatTimeout = time.Now().UnixMilli() + rf.heartbeatDuration
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

	rf.heartbeatDuration = 100 // 100ms
	rf.electionChan = make(chan bool)
	rf.heartbeatChan = make(chan bool)
	rf.State = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rand.Seed(time.Now().UnixNano())
	rf.ctx = context.WithValue(context.Background(), "logID", rand.Intn(20))
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.electionTimeout = time.Now().UnixMilli() + 150
	rf.resetTimerElection()
	go rf.mainLoop()
	go rf.timerElection()
	go rf.timerHeartbeat()
	return rf
}
