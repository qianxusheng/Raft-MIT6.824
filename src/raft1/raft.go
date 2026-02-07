package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

const (
	Follower  = 0
	Candidate = 1
	Leader    = 2
)

// 一些thoughts
// labA:
// 1. raft是peer to peer的；之前做的mapreduce是经典的master-slave架构
// 2. heartbeat(leader还活着)，startElection(防止重复发起)，requestVote(还在选举)需要注意重置lastHeartBeat
// 3. 每个raft在每个term只能投一次
// 4. heartbeat and electionTimeout的trade-off；如果heartbeat太长，每次都要重新选举，太短就是网络拥挤

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state         int
	lastHeartbeat time.Time
	// persist on disk
	currentTerm int // logical clock，用于处理unreliable的网络问题，保证RPC有序
	votedFor    int
	log         []Entry

	// volatile on rafts
	commitIndex int // committed by most follower
	lastApplied int // command applied by the service using raft

	// volatile on leaders
	nextIndex  []int // 记录follower下一个需要什么
	matchIndex []int // 记录follower当前match到了哪里，如果多数match了某个index，就可以commit了

	// applier goroutine，给KV layer同步阻塞的报告信息，因为在同一machine，所以不用RPC
	applyCh   chan raftapi.ApplyMsg
	applyCond *sync.Cond
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = (rf.state == Leader)

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

type Entry struct {
	Term    int
	Command interface{}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// RPC handler, need to be captialized
// 检测冲突
// 修复冲突
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false
	// reject，只有合法leader才能镇压suppress
	if args.Term < rf.currentTerm {
		return
	}

	// heartbeat
	rf.lastHeartbeat = time.Now()
	rf.currentTerm = args.Term
	rf.state = Follower

	// prevLog一致性检查, prevLog consistency check
	if args.PrevLogIndex >= len(rf.log) { // log太短，follower起码能有这么长的log
		return
	}
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm { // term不匹配
		return
	}

	// 追加entries（不一致的还需要截断，prevLog检查只保证前面的是一致的）
	// 特别针对于partition问题，follower连回来多了很多不对的log
	// 如果是normal case，这个raft机制可以通过induction证明prevLog之前的都是对的（对的在raft里面==多数的log）
	for i, entry := range args.Entries {
		// 从前开始，发现不对直接截断，然后append后面的args.Entries
		idx := args.PrevLogIndex + 1 + i
		if idx < len(rf.log) { // 这个是anomaly
			if rf.log[idx].Term != entry.Term {
				rf.log = rf.log[:idx]                        // 截至idx
				rf.log = append(rf.log, args.Entries[i:]...) // append正确的
				break
			}
		} else {
			rf.log = append(rf.log, args.Entries[i:]...) // 这是normal case
			break
		}
	}

	// follower要维护自己的volatile变量
	// commitIndex->pull
	if args.LeaderCommit > rf.commitIndex { // commitIndex单调增加
		// 如果leaderCommit大，说明当前follower很落后在追赶，log一共才这么点，所以commitIndex也就这么点
		// 如果rf.log大，很正常，说明rf.log>Leadercommit的部分还没有被majority commit
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
		rf.applyCond.Signal()
	}
	reply.Success = true
}

// 当前是leader才调用
// 有4种作用
// push->commitIndex
func (rf *Raft) sendAppendEntries() {
	for i := range rf.peers {
		if i != rf.me {
			prevLogIndex := rf.nextIndex[i] - 1
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  rf.log[prevLogIndex].Term,
				Entries:      rf.log[rf.nextIndex[i]:],
				LeaderCommit: rf.commitIndex,
			}
			go func(server int) {
				reply := AppendEntriesReply{}
				ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
				// 1. heartbeat
				// 如果当前followe已经up-to-date了，再发一次RPC就相当于heartbeat
				// 反正就是appendentry RPC就会让follower保持follower
				// rf.state = Follower
				// rf.lastHeartbeat = time.Now()
				//...

				// 2. 网络问题的retry
				// 如果RPC网络问题，retry，然后ticker就再次发送，相当于重新发送entry
				if !ok {
					return
				}

				rf.mu.Lock() // 锁加在RPC外面
				defer rf.mu.Unlock()

				// 确保raft还是当前term的leader
				if rf.currentTerm != args.Term || rf.state != Leader {
					return
				}

				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.state = Follower
					rf.votedFor = -1
				}

				// 处理reply
				if reply.Success {
					// 3.append成功
					// 成功，append entry，更新leader维护的相关状态
					// 然后判断有没有commit成功，还需要返回给kv layer
					rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
					rf.matchIndex[server] = rf.nextIndex[server] - 1

					// 检查是否可以commit
					// 倒序查找，还是挺高效的，批量commit
					for n := len(rf.log) - 1; n > rf.commitIndex; n-- {
						count := 1
						for i := range rf.peers {
							if i != rf.me && rf.matchIndex[i] >= n {
								count++
							}
						}

						if count > len(rf.peers)/2 {
							rf.commitIndex = n
							rf.applyCond.Signal()
							break
						}
					}
				} else {
					// 4. append失败
					// 回退，找到match点
					rf.nextIndex[server]--
				}
			}(i)
		}
	}
}

// 分析case的时候需要先保证这种case发生，然后handle这种case产生的问题
// 略有担心未来的leader抢不到startElection
// 听说随机化能解决这个问题（概率问题？也不是概率问题）
// 不用担心，辣鸡candidate被reject后进入follower，然后electiontimeout，就不会捣乱了
// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	// logical clock, prevent outdated candidates
	// 僵尸candidate，reject
	if args.Term < rf.currentTerm {
		return
	}

	// case1:
	// s1: term 10, [1, 1, 1]
	// ---------------------- partition
	// s2(leader): term 5, [1, 1, 1, 4, 4]
	// leader还是要退位，进行下一轮选举（还是能赢）
	// Term是逻辑时钟，要同步
	// case2:
	// 正常情况下，s1(term)就要投给s2(term + 1)，然后更新term和votedFor（这一轮投过了）
	// case3:
	// 两个candidate同时发起election，打成平手，互相要票->看谁的log比较完整
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
		reply.Term = rf.currentTerm
	}

	// 可能已经投过别人了
	// case4：
	// 当前raft已经投过别人了，然后term变成了5（当前最新term）
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		return
	}

	// case5:网络问题
	// s1: 3 4 4 4  假设s1恢复网络然后发起选举，发现自己term和s2相等
	// ------------
	// s2（多数）：3 4 4 5  但是s1拿不到leader，s1timeout，然后s2拿到，一切恢复正常
	// case6
	// s1（多数）: 3 4 4 4
	// -----------
	// s2: 3 4 4 5   这个case不可能发生，因为虽然term++，但是永远不会写入
	// election restriction
	myLastLogIndex := len(rf.log) - 1
	myLastLogTerm := 0
	if myLastLogIndex >= 0 {
		myLastLogTerm = rf.log[myLastLogIndex].Term
	}
	upToDate := args.LastLogTerm > myLastLogTerm ||
		((args.LastLogTerm == myLastLogTerm) && (args.LastLogIndex >= myLastLogIndex))
	if !upToDate {
		return
	}

	// grant vote
	rf.state = Follower
	rf.lastHeartbeat = time.Now()
	rf.votedFor = args.CandidateId
	reply.VoteGranted = true
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

	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return -1, rf.currentTerm, false
	}

	// only leader reply to client
	// leader append log
	// send AE RPC to followers
	index = len(rf.log)
	term = rf.currentTerm
	rf.log = append(rf.log, Entry{Term: rf.currentTerm, Command: command})
	rf.sendAppendEntries() // try to make agreement

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
	rf.applyCond.Signal() // 唤醒applier，killed自己
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// ticker: 轮询检测超时
// 分布式系统无法依赖event-driven，因为故障节点不会主动通知
// 1. send appeneEntry periodically
// if no new entry: heartbeat
// if PRC lost: retry
// if RPC false: fix
// if RPC true: commit if majority agrees
// 2. leader selection
func (rf *Raft) ticker() {
	for rf.killed() == false {
		rf.mu.Lock()
		// Your code here (3A)
		// Check if a leader election should be started.
		// leader需要定期发送heartbeat镇压follower
		// 通过发送AE RPC实现，把candidate修改成follower，重置
		if rf.state == Leader {
			rf.sendAppendEntries()
		} else if rf.state != Leader {
			if time.Since(rf.lastHeartbeat) > time.Duration(300+rand.Intn(200))*time.Millisecond {
				go rf.startElection()
			}
		}
		rf.mu.Unlock()
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		time.Sleep(100 * time.Millisecond)
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.currentTerm += 1 // 不+=1，term和其他的follower一样，拉不到票
	rf.votedFor = rf.me
	// rf.lastHeartbeat = time.Now()很重要！！
	// 防止当前raft重新开始选举，比如有人直接被reject，然后进入electiontimeout
	rf.lastHeartbeat = time.Now()
	rf.state = Candidate
	term := rf.currentTerm
	rf.mu.Unlock()

	vote := 1 // 给自己vote；这里的vote会被spawn的gproutine捕获，所以修改这个vote需要lock
	for i := range rf.peers {
		if i != rf.me {
			// spawn goroutine防止阻塞
			go func(server int) {
				rf.mu.Lock()
				lastLogIndex := len(rf.log) - 1
				lastLogTerm := 0
				if lastLogIndex >= 0 {
					lastLogTerm = rf.log[lastLogIndex].Term
				}
				rf.mu.Unlock()
				args := &RequestVoteArgs{Term: term, CandidateId: rf.me, LastLogIndex: lastLogIndex, LastLogTerm: lastLogTerm}
				reply := &RequestVoteReply{}
				ok := rf.sendRequestVote(server, args, reply)
				if !ok { // 忽略这一票
					return
				}

				// 处理reply
				rf.mu.Lock()
				defer rf.mu.Unlock()
				// term不同，就是换了一轮选举了（不管是不是自己发起的），reply过时了，直接return不处理了
				if rf.state != Candidate || rf.currentTerm != term {
					return
				}
				if reply.Term > rf.currentTerm { // 如果自己term小，变回follower
					rf.state = Follower
					rf.votedFor = -1
					rf.currentTerm = reply.Term // 更新term；我们可以直接强制转换，后续处理好uncommited log
					return
				}
				if reply.VoteGranted { // 得票，+=1
					vote += 1
					// 变成leader之后要重置
					if vote > len(rf.peers)/2 {
						rf.state = Leader
						// volateile on leaders, 所以leader变化也要重置
						for i := range rf.peers {
							rf.nextIndex[i] = len(rf.log)
							rf.matchIndex[i] = 0
						}
						rf.sendAppendEntries()
					}
					return
				}
			}(i)
		}
	}
}

// 每个raft都有，写入自己的KV layer
// apply msg to the upper layer
func (rf *Raft) applier() {
	for rf.killed() == false {
		rf.mu.Lock()
		// 本质就是消费者-生产者模型
		for rf.lastApplied >= rf.commitIndex { // 和条件有关的都要写signal
			rf.applyCond.Wait()
		}
		rf.lastApplied++
		msg := raftapi.ApplyMsg{
			CommandValid: true,
			Command:      rf.log[rf.lastApplied].Command,
			CommandIndex: rf.lastApplied,
		}
		rf.mu.Unlock()
		rf.applyCh <- msg
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
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.state = Follower
	rf.votedFor = -1
	rf.currentTerm = 0
	rf.lastHeartbeat = time.Now()
	rf.log = []Entry{}
	rf.log = append(rf.log, Entry{Term: 0, Command: "dummy entry"})

	// volatile
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	// start applier
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)
	go rf.applier()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
