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
	"log"
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
	currentTerm int  // 服务器当前的任期
	votedFor    *int // 当前任期内收到选票的候选人 如果没有投给任何候选人 则为空
	//TimeOut     time.Time // 超时时间
	State int // 当前节点的状态

	electionChan  chan bool
	heartbeatChan chan bool

	electionTimeout  int64 // 发起选举超时时间
	electionDuration int64
	timeoutHeartbeat int64 // leader发送心跳包的时间
	heartbeatTimeout int64 // 发送心跳包

	ctx context.Context
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
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
	Term       int // 当前任期
	Success    bool
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
	VoteGranted bool // 当候选人赢得了此章选票时为真
	Term        int  // 当前任期号
	FollowerId  int  // 投票人的id
}

// RequestVote 是follower收到candidate要求投票的rpc
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.State = Follower
		rf.currentTerm = args.Term
	}

	if args.Term < rf.currentTerm || rf.votedFor != nil {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		reply.FollowerId = rf.me
		return
	}

	if rf.votedFor == nil {
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		reply.FollowerId = rf.me

		rf.votedFor = &args.CandidateId
		rf.resetTimerElection()

	}
}

// AppendEntries 接受到AppendEntries后
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if args.LeaderTerm < rf.currentTerm {
		// 领导人的任期比当前的任期小 返回假
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.FollowerID = rf.me
		return
	}

	// 重置超时选举时间
	rf.resetTimerElection()

	if args.LeaderTerm > rf.currentTerm || rf.State != Follower {
		rf.currentTerm = args.LeaderTerm
		rf.State = Follower
	}

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

	rf.resetTimerElection()

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go func(id int) {
			args := AppendEntriesArgs{}
			args.LeaderTerm = rf.currentTerm
			args.LeaderID = rf.me

			reply := AppendEntriesReply{}
			if rf.sendAppendEntries(id, &args, &reply) {
				// 广播的心跳包被follower接受到了
				if reply.Success {
					log.Printf("cur leader %v", rf.me)
				} else {
					// 领导人的任期小于接受者的任期
					rf.State = Follower
				}
			} else {
				log.Printf("")
			}
		}(i)
	}
}

// startElection 开始选举流程
func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.State = Candidate
	rf.currentTerm = rf.currentTerm + 1

	votes := 1 // 自己投自己一票
	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go func(id int) {
			rf.mu.Lock()
			// 发送广播
			args := RequestVoteArgs{}
			args.Term = rf.currentTerm
			args.CandidateId = rf.me
			rf.mu.Unlock()

			var reply RequestVoteReply

			if rf.sendRequestVote(id, &args, &reply) {

				if rf.currentTerm < reply.Term {
					log.Printf("currentTerm is %v,votedTerm is %v,Failed vote", rf.currentTerm, reply.Term)
					rf.State = Follower
					rf.currentTerm = reply.Term
					return
				}

				if reply.VoteGranted {
					votes++

					if votes > len(rf.peers)/2 && rf.State == Candidate {
						log.Printf("%d is Leader", rf.me)
						// 成为leader
						rf.State = Leader
						// 发送广播包 leader当选
						rf.sendHeartbeat()
					}
				} else {
					log.Printf("logID = %v,scurID %v,curTerm %v,votedId is %v votedTerm is %v,reject voted",
						rf.ctx.Value("logID"), rf.me, rf.currentTerm, reply.FollowerId, reply.Term)
				}
			} else {
				// 发送投票失败
				log.Printf("rpc error %v", id)
			}

		}(i)
	}

}

// resetTimerElection 重置超时选举时间
func (rf *Raft) resetTimerElection() {
	rand.Seed(time.Now().UnixNano())
	rf.electionDuration = rand.Int63n(150) + rf.timeoutHeartbeat*5
	rf.electionTimeout = time.Now().UnixMilli() + rf.electionDuration
}

// 超时后向chan中发送消息 然后开始选举
func (rf *Raft) timerElection() {
	for {
		timeNow := time.Now().UnixMilli()
		if timeNow-rf.electionTimeout > 0 {
			log.Printf("logID = %v,raft id = %d timeout, current term %v,current status %v",
				rf.ctx.Value("logID"), rf.me, rf.currentTerm, StateMapping[rf.State])
			rf.electionChan <- true
		}
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
	rf.heartbeatTimeout = time.Now().UnixMilli() + rf.heartbeatTimeout
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

	rf.timeoutHeartbeat = 100 // 100ms
	rf.electionChan = make(chan bool)
	rf.heartbeatChan = make(chan bool)
	rf.State = Follower
	rf.currentTerm = 0
	rf.votedFor = nil
	rand.Seed(time.Now().UnixNano())
	rf.ctx = context.WithValue(context.Background(), "logID", rand.Int())
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.resetTimerElection()
	go rf.mainLoop()
	go rf.timerElection()
	go rf.timerHeartbeat()
	return rf
}
