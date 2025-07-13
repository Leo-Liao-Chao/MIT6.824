package raft

import (
	"bytes"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
)

// Step 1: 定义 Raft 角色类型
// 定义角色类型
const LEADER = "Leader"
const CANDIDATE = "Candidate"
const FOLLOWER = "Follower"

// Step 2: 定义 Raft 结构体
type Raft struct {
	mu        sync.Mutex          // 互斥锁保护状态
	peers     []*labrpc.ClientEnd // 服务器列表
	persister *Persister          // 持久化存储
	me        int                 // 当前服务器的索引
	dead      int32               // 是否已死

	// 必须持久化的状态:
	currentTerm int        // 当前任期
	votedFor    int        // 当前任期内投票给的服务器
	log         []LogEntry // 日志条目，每一条日志对应都是一个客户端命令
	/*  只保留快照和快照之后的新日志，以节省内存
	日志结构（压缩前）：
	Index:   [1]  [2]  [3]  [4]  [5]  [6]
	Term:    [1]  [1]  [2]  [2]  [3]  [3]
			↓ 快照截断到 Index=3, Term=2
	快照：包含 [1][2][3] 的状态效果
	压缩后日志结构：
	lastIncludedIndex = 3
	lastIncludedTerm = 2
	日志数组只保留从 Index=4 开始的新日志
	*/
	lastIncludedIndex int // 当前快照中，最后包含的日志索引
	lastIncludedTerm  int // 当前快照中，最后包含的日志任期

	// 容易丢失的状态（运行时状态）:
	commitedIndex int // 已提交的日志索引：Leader 在将某条日志成功复制到超过半数的节点后，会将该条日志视为“已提交”，并更新 commitedIndex。
	lastApplied   int // 最后应用的日志索引：Raft 节点会从 lastApplied + 1 到 commitedIndex，逐条将日志通过 applyCh 发送到上层服务（状态机）中执行。

	// Leader 状态
	/*Leader 每次发送 AppendEntries RPC 时，会参考这个值决定从哪里开始发送日志给 follower。
	  如果 follower 返回失败（日志不匹配），Leader 会回退 nextIndex[i]--，直到匹配为止。
	*/
	nextIndex []int // 每个服务器的下一个日志索引：nextIndex[i] 表示 Leader 准备发送给该 follower 的下一条日志条目的索引。
	/*----------------------------------------------------------------*/
	// Leader 找出一个 N，满足：
	// N > commitIndex
	// 有多数节点的 matchIndex >= N
	// log[N].term == currentTerm（必须是当前 term 的日志，防止旧日志覆盖新日志）
	matchIndex []int // 每个服务器的已匹配日志索引：对于每一个 follower，matchIndex[i] 表示 Leader 已知该 follower 已经成功复制的最大日志索引。

	// 其他状态:
	role              string    // 当前角色
	leaderId          int       // 当前领导者的索引
	lastActiveTime    time.Time // 最后活跃时间，用于 Follower 判断 Leader 是否失联，是否发起选举。
	lastBroadcastTime time.Time // 最后广播时间，用于 Leader 广播心跳，用于控制心跳间隔（比如每 100ms 发一次心跳）。

	// chan 一个线程安全的管道/消息队列，多个生产者/消费者可以同时操作
	applyCh chan ApplyMsg // 应用消息通道，应用日志或快照到上层状态机的通道；
}

// Step 2.1: 定义日志条目结构体（Leader 接收客户端命令）
type LogEntry struct {
	Term    int         // 日志条目的任期
	Command interface{} // 日志条目的命令
}

// Step 2.2: 定义 ApplyMsg 结构体（应用到状态机的关键机制）
type ApplyMsg struct {
	CommandValid bool        // 是否是有效的命令
	Command      interface{} // 命令内容
	CommandIndex int         // 命令索引
	CommandTerm  int         // 命令任期

	Snapshot          []byte // 快照数据
	LastIncludedIndex int    // 快照包含的最后日志索引
	LastIncludedTerm  int    // 快照包含的最后日志任期
}

// Step 3: Make 创建 Raft 实例
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:     peers,
		persister: persister,
		me:        me,
		applyCh:   applyCh, // 应用消息通道

		role:              FOLLOWER,   // 初始角色为 Follower
		leaderId:          -1,         // 初始没有领导者
		votedFor:          -1,         // 初始没有投票
		lastIncludedIndex: 0,          // 初始快照索引为 0
		lastIncludedTerm:  0,          // 初始快照任期为 0
		lastActiveTime:    time.Now(), // 初始最后活跃时间为当前时间
	}

	// 恢复状态
	rf.readPersist(persister.ReadRaftState()) // 加载状态，恢复持久化数据
	rf.installSnapshotToApplication()         // 安装快照，应用层

	// 启动 Raft 服务器
	go rf.electionLoop()      // 启动选举循环
	go rf.appendEntriesLoop() // 启动日志复制循环
	go rf.applyLogLoop()      // 启动日志应用循环

	return rf
}

// Step 3.1 electionLoop 选举循环
func (rf *Raft) electionLoop() {
	for !rf.killed() {
		time.Sleep(10 * time.Millisecond) // 防止 CPU 占用过高，间隔10 ms

		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			now := time.Now()
			// 如果多个节点在同一时间发起选举，它们很可能互相投票，导致没有任何节点获得多数票，从而进入选举僵局，频繁重新选举。为了避免这种情况，可以引入随机化选举超时时间，使得不同节点发起选举的时间错开。计算有一个随机的先后顺序，而不是所有节点同时发起选举。
			timeout := time.Duration(200+rand.Int31n(150)) * time.Millisecond // 超时时间
			elapses := now.Sub(rf.lastActiveTime)                             // 计算和上一次活跃时间的时间差

			// Follower ----> Candidate
			// 超时后，准备发起选举，角色变为 Candidate
			if rf.role == FOLLOWER && elapses >= timeout {
				rf.role = CANDIDATE // 角色变为 Candidate
			}

			// Candidate ----> Leader
			// 发起选举
			if rf.role == CANDIDATE && elapses >= timeout {
				rf.lastActiveTime = now // 更新最后活跃时间

				rf.currentTerm++    // 增加任期
				rf.votedFor = rf.me // 投票给自己
				rf.persist()        // 持久化状态

				args := RequestVoteArgs{
					Term:         rf.currentTerm,
					CandidateId:  rf.me,
					LastLogIndex: rf.lastIndex(),
					LastLogTerm:  rf.lastTerm(),
				}

				rf.mu.Unlock() // 释放锁，防止阻塞

				type VoteResult struct {
					peerId int               // 服务器 ID
					reply  *RequestVoteReply // 回复
				}
				voteCount := 1                                          // 自己投票给自己
				finishCount := 1                                        // 收到回复的个数
				voteResultChan := make(chan *VoteResult, len(rf.peers)) // 创建一个通道，（缓存长度未len）用于接收投票结果

				for peerId := range rf.peers {
					go func(id int) {
						if id == rf.me {
							return
						}
						reply := RequestVoteReply{}
						ok := rf.sendRequestVote(id, &args, &reply)
						if ok {
							// <- 往通道中写入数据，如果通道中没有数据，则阻塞等待
							voteResultChan <- &VoteResult{peerId: id, reply: &reply}
						} else {
							voteResultChan <- &VoteResult{peerId: id, reply: nil}
						}
					}(peerId)
				}

				maxTerm := 0
			VOTE_CHECK:
				for {
					// slect 用于监听多个通道，当某个通道有数据可读时，就会执行相应的代码块
					select {
					case voteResult := <-voteResultChan:
						// 收到回应计数
						finishCount++

						if voteResult.reply != nil {
							// 收到投票
							if voteResult.reply.VoteGranted {
								voteCount++
							}
							// 发现任期大于当前任期，更新
							if voteResult.reply.Term > rf.currentTerm {
								maxTerm = voteResult.reply.Term
							}
						}

						if finishCount == len(rf.peers) || voteCount > len(rf.peers)/2 {
							break VOTE_CHECK
						}
					}
				}

				rf.mu.Lock() // 加锁

				if rf.role != CANDIDATE {
					return
				}

				if maxTerm > rf.currentTerm {
					rf.role = FOLLOWER       // 任期大于当前任期，变为 Follower
					rf.currentTerm = maxTerm // 更新任期
					rf.votedFor = -1         // 投票无效
					rf.leaderId = -1         // 没用领导者
					rf.persist()
					return
				}

				if voteCount > len(rf.peers)/2 {
					rf.role = LEADER                          // 获得多数票，成为 Leader
					rf.leaderId = rf.me                       // 更新领导者
					rf.nextIndex = make([]int, len(rf.peers)) // 初始化 nextIndex
					for i := 0; i < len(rf.peers); i++ {
						rf.nextIndex[i] = rf.lastIndex() + 1 // 初始化 nextIndex
					}
					rf.matchIndex = make([]int, len(rf.peers)) // 初始化 matchIndex
					for i := 0; i < len(rf.peers); i++ {
						rf.matchIndex[i] = 0 // 初始化 matchIndex
					}
					rf.lastBroadcastTime = time.Unix(0, 0) // 令appendEntries广播立即执行
					return
				} else {
					rf.role = FOLLOWER // 没有获得多数票，变为 Follower
					rf.votedFor = -1   // 投票无效
					rf.leaderId = -1   // 没有领导者
					rf.persist()
					return
				}

			}

		}()

	}
}

// Step 3.2 appendEntriesLoop 日志复制循环
func (rf *Raft) appendEntriesLoop() {
	for !rf.killed() {
		time.Sleep(10 * time.Millisecond) // 防止 CPU 占用过高，间隔10 ms

		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			// 只有Leader才能向外，追加日志
			if rf.role != LEADER {
				return
			}

			now := time.Now()
			// 大于100 ms 才广播一次
			if now.Sub(rf.lastBroadcastTime) < time.Duration(100)*time.Millisecond {
				return
			}
			rf.lastBroadcastTime = time.Now()

			for peerId := range rf.peers {
				if peerId == rf.me {
					continue
				}
				// nextIndex[peerId] 表示下一个要发送给 peerId 的日志条目的索引
				// lastIncludedIndex 表示已经保存的日志最后索引

				if rf.nextIndex[peerId] <= rf.lastIncludedIndex {
					// 对方落后，Leader 没有那么旧的日志了
					rf.doInstallSnapshot(peerId)
				} else {
					//对方不落后，Leader 可以发日志来同步
					rf.doAppendEntries(peerId)
				}
			}
		}()
	}
}

// Step 4: DeTail function

// Step 4.1: Vote
type RequestVoteArgs struct {
	Term         int // 候选人的任期
	CandidateId  int // 候选人的 ID
	LastLogIndex int // 候选人最后日志条目的索引
	LastLogTerm  int // 候选人最后日志条目的任期
}

type RequestVoteReply struct {
	Term        int  // 当前任期，以便候选人更新自己的任期
	VoteGranted bool // 是否投票
}

// 调用 RPC
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 初始化 Reply
	reply.Term = rf.currentTerm // 返回当前任期
	reply.VoteGranted = false   // 默认不投票

	// 1. 如果任期小于当前任期，拒绝投票
	if args.Term < rf.currentTerm {
		return
	}

	// 2. 如果任期大于当前任期，更新任期，并变为 Follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = FOLLOWER
		rf.votedFor = -1 //准备投票
		rf.leaderId = -1 //准备投票
	}

	// 3. 还没投票，或者，投票的Id是request 的候选者 Id，投票
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		lastLogTerm := rf.lastTerm()

		if args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= rf.lastIndex()) {
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
			rf.lastActiveTime = time.Now() // 更新自己存活时间。
		}
	}
	rf.persist() // 持久化状态
}

// Step 4.2: AppendEntries

// Step 4.2.1: AppendEntries
type AppendEntriesArgs struct {
	Term         int        // 领导人的任期
	LeaderId     int        // 领导人的 Id
	PrevLogIndex int        // 新的日志条目紧接前一条的索引
	PrevLogTerm  int        // 新的日志条目的任期
	Entries      []LogEntry // 准备存储的日志条目，表示心跳时为空
	LeaderCommit int        // 领导人已经提交的日志的索引
}

type AppendEntriesReply struct {
	Term          int  // 当前任期，以便领导人更新自己的任期
	Success       bool // 日志是否匹配
	ConflictIndex int  // 冲突日志条目的索引
	ConflictTerm  int  // 冲突日志条目的任期
}

// 调用 RPC
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// Follower 执行
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 初始化 Reply
	reply.Term = rf.currentTerm // 返回当前任期
	reply.Success = false       // 默认不成功
	reply.ConflictIndex = -1    // 初始化冲突日志条目的索引
	reply.ConflictTerm = -1     // 初始化冲突日志条目的任期

	// 1. 如果任期小于当前任期，拒绝添加日志
	if args.Term < rf.currentTerm {
		return
	}
	// 2. 如果任期大于当前任期，更新任期，并变为 Follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = FOLLOWER
		rf.votedFor = -1
		rf.persist()
	}

	rf.leaderId = args.LeaderId
	rf.lastActiveTime = time.Now() // 更新自己存活时间。

	// AppendEntries
	// case 1：发现Index在last之前，异常
	if args.PrevLogIndex < rf.lastIncludedIndex {
		reply.ConflictIndex = 1
		return
		// case 2：发现Index匹配, Term 不匹配
	} else if args.PrevLogIndex == rf.lastIncludedIndex {
		// Term 不匹配
		if args.PrevLogTerm != rf.lastIncludedTerm {
			reply.ConflictIndex = 1
			return
		}
	} else {
		// case 3：PrevLogIndex > lastIncludeIndex
		if args.PrevLogIndex > rf.lastIndex() {
			reply.ConflictIndex = rf.lastIndex() + 1
			return
		}
		// 找到冲突的日志的位置
		if rf.log[rf.index2LogPos(args.PrevLogIndex)].Term != args.PrevLogTerm {
			reply.ConflictTerm = rf.log[rf.index2LogPos(args.PrevLogIndex)].Term
			for index := rf.lastIncludedIndex + 1; index <= args.PrevLogIndex; index++ {
				if rf.log[rf.index2LogPos(index)].Term == reply.ConflictTerm {
					reply.ConflictIndex = index
					break
				}
			}
			return
		}
	}
	// 合法的位置
	for i, logEntry := range args.Entries {
		index := args.PrevLogIndex + i + 1
		logPos := rf.index2LogPos(index)
		// 超出Log长度
		if index > rf.lastIndex() {
			rf.log = append(rf.log, logEntry)
		} else {
			// 任期不一致
			if rf.log[logPos].Term != logEntry.Term {
				rf.log = rf.log[:logPos]          // 保留冲突之前的log
				rf.log = append(rf.log, logEntry) // 添加新的log
			}

		}
	}
	rf.persist()

	// 更新提交了的日志长度
	if args.LeaderCommit > rf.commitedIndex {
		rf.commitedIndex = min(args.LeaderCommit, rf.lastIndex())
	}
	reply.Success = true
}

func (rf *Raft) doAppendEntries(peerId int) {
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		LeaderCommit: rf.commitedIndex,
		Entries:      make([]LogEntry, 0),
		PrevLogIndex: rf.nextIndex[peerId] - 1,
	}
	// 确认PreLogTerm
	// 如果他是最后一个日志，那么PrevLogTerm就是lastIncludedTerm
	if args.PrevLogIndex == rf.lastIncludedIndex {
		args.PrevLogTerm = rf.lastIncludedTerm
	} else {
		// 不是最后一个日志，就对应log中的term
		args.PrevLogTerm = rf.log[rf.index2LogPos(args.PrevLogIndex)].Term
	}
	//  append(args.Entries, ...) 表示把 args.Entries 和 rf.log[rf.index2LogPos(args.PrevLogIndex)+1:] 合并
	// rf.log[rf.index2LogPos(args.PrevLogIndex)+1:] 表示从 prevLogIndex 的下一条开始，到未保存的日志结束
	// Go 中的 ... 是展开 slice 的语法，把 []LogEntry 拆开传入 append()。
	args.Entries = append(args.Entries, rf.log[rf.index2LogPos(args.PrevLogIndex)+1:]...)

	go func() {
		reply := AppendEntriesReply{}
		ok := rf.sendAppendEntries(peerId, &args, &reply)
		if ok {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			// 如果任期不一致，直接返回
			if rf.currentTerm != args.Term {
				return
			}
			// 发现任期落后，变为Follower
			if reply.Term > rf.currentTerm {
				rf.role = FOLLOWER
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.leaderId = -1
				rf.persist()
				return
			}
			// 如果恢复正常
			if reply.Success {
				// 更新下一次发送日志的位置
				rf.nextIndex[peerId] = args.PrevLogIndex + len(args.Entries) + 1
				// 对上了的日志
				rf.matchIndex[peerId] = args.PrevLogIndex + len(args.Entries)
				// 更新提交索引
				rf.updateCommitIndex()
			} else {
				// 回退版本

				if reply.ConflictTerm != -1 {
					// 有版本冲突，然后更新他的nextIndex为未冲突的Index
					conflictTermIndex := -1 // 初始化冲突任期
					for index := args.PrevLogIndex; index > rf.lastIncludedIndex; index-- {
						if rf.log[rf.index2LogPos(index)].Term == reply.ConflictTerm {
							conflictTermIndex = index
							break
						}
					}
					if conflictTermIndex != -1 {
						rf.nextIndex[peerId] = conflictTermIndex // 找到了冲突的Term
					} else {
						rf.nextIndex[peerId] = reply.ConflictIndex // 没有找到Term, 用follower的Term 的index作为开始
					}
				} else {
					rf.nextIndex[peerId] = reply.ConflictIndex // 没有找到Term, 用follower的conflictIndex作为开始
				}
			}
		}
	}()
}

// Step 4.2.2：InstallSnapshot
type InstallSnapshotArgs struct {
	Term              int    // 领导人的任期
	LeaderId          int    // 领导人的 Id
	LastIncludedIndex int    // 已保存日志最后索引
	LastIncludedTerm  int    // 已保存日志最后任期
	Offset            int    // 数据起始位置
	Data              []byte // 快照数据
	Done              bool   // 是否全部发送
}

type InstallSnapshotReply struct {
	Term int // 当前任期，以便领导人更新自己的任期
}

// RPC
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	// 如果任期落后，直接返回
	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.role = FOLLOWER
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	rf.leaderId = args.LeaderId
	rf.lastActiveTime = time.Now()

	// 小于保存的日志
	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		return
	} else {
		// 还有未保存的日志
		if args.LastIncludedIndex < rf.lastIndex() {
			if rf.log[rf.index2LogPos(args.LastIncludedIndex)].Term != args.LastIncludedTerm {
				// 丢掉没保存的日志
				rf.log = make([]LogEntry, rf.lastIndex()-args.LastIncludedIndex)
			} else {
				leftlog := make([]LogEntry, rf.lastIndex()-args.LastIncludedIndex)
				copy(leftlog, rf.log[rf.index2LogPos(args.LastIncludedIndex)+1:])
				rf.log = leftlog
			}
		} else {
			rf.log = make([]LogEntry, 0) // 快照的Index >=本地
		}
	}
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.persister.SaveStateAndSnapshot(rf.persister.ReadRaftState(), args.Data)
	rf.installSnapshotToApplication()
}

func (rf *Raft) doInstallSnapshot(peerId int) {

	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Offset:            0,
		Data:              rf.persister.ReadSnapshot(),
		Done:              false,
	}

	reply := InstallSnapshotReply{}

	go func() {
		if rf.sendInstallSnapshot(peerId, &args, &reply) {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.currentTerm != args.Term {
				return
			}
			if reply.Term > rf.currentTerm {
				rf.role = FOLLOWER
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.leaderId = -1
				rf.persist()
				return
			}
			// 结合判断条件来看
			rf.nextIndex[peerId] = rf.lastIndex() + 1
			rf.matchIndex[peerId] = args.LastIncludedIndex
			rf.updateCommitIndex()
		}
	}()
}

// Step 4.3: ApplyLog
func (rf *Raft) applyLogLoop() {
	noMore := false
	for !rf.killed() {
		if noMore {
			time.Sleep(10 * time.Millisecond)
		}
		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			noMore = true
			// 有新的提交的数据，应用可以更新了
			if rf.commitedIndex > rf.lastApplied {
				rf.lastApplied++
				appliedIndex := rf.index2LogPos(rf.lastApplied)
				appliedMsg := ApplyMsg{
					CommandValid: true,
					Command:      rf.log[appliedIndex].Command,
					CommandIndex: rf.lastApplied,
					CommandTerm:  rf.log[appliedIndex].Term,
				}
				rf.applyCh <- appliedMsg
				noMore = false
			}
		}()
	}
}

// Step 5: Start
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != LEADER {
		isLeader = false
		return index, term, isLeader
	}

	logEntry := LogEntry{
		Command: command,
		Term:    rf.currentTerm,
	}
	rf.log = append(rf.log, logEntry)
	index = rf.lastIndex()
	term = rf.currentTerm
	rf.persist()

	return index, term, isLeader
}

// Tools：工具类函数
// 将Raft实例的状态设置为已死
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm, rf.role == LEADER
}
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1) // 原子操作，设置 dead 为 1
}

// 检查 Raft 实例是否已死
func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead) // 原子操作，读取 dead 的值
	return z == 1
}

// 返回最后一条日志的索引
func (rf *Raft) lastIndex() int {
	return rf.lastIncludedIndex + len(rf.log) // 已经保存的日志最后索引+未保存的日志
}

// 返回最后一条日志的任期
func (rf *Raft) lastTerm() int {
	if len(rf.log) == 0 {
		return rf.lastIncludedTerm // 已经保存的日志的最后任期
	}
	return rf.log[len(rf.log)-1].Term // 未保存的日志的最后任期（最新）
}

// index to Log数组下标 (index-lastIncludeIndex -1)
func (rf *Raft) index2LogPos(index int) int {
	return index - rf.lastIncludedIndex - 1
}

// 更新提交的Index

func (rf *Raft) updateCommitIndex() {
	sortedMatrchIndex := make([]int, 0)
	sortedMatrchIndex = append(sortedMatrchIndex, rf.lastIndex())
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		sortedMatrchIndex = append(sortedMatrchIndex, rf.matchIndex[i])
	}
	sort.Ints(sortedMatrchIndex)
	// 找到已经提交的日志的中位数
	newCommitIndex := sortedMatrchIndex[len(sortedMatrchIndex)/2]
	// 如果提交的中位数是有效的
	if newCommitIndex > rf.commitedIndex && (newCommitIndex <= rf.lastIncludedIndex || rf.log[rf.index2LogPos(newCommitIndex)].Term == rf.currentTerm) {
		rf.commitedIndex = newCommitIndex
	}

}

// 获取保存的数据
func (rf *Raft) raftStateForPersist() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	return w.Bytes()
}

// 保存数据
func (rf *Raft) persist() {
	data := rf.raftStateForPersist()
	rf.persister.SaveRaftState(data)
}

// 读取数据
func (rf *Raft) readPersist(data []byte) {
	// 无效数据
	if data == nil || len(data) < 1 {
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
	d.Decode(&rf.lastIncludedIndex)
	d.Decode(&rf.lastIncludedTerm)
}

// 把数据传递给应用层
func (rf *Raft) installSnapshotToApplication() {
	applyMsg := ApplyMsg{
		CommandValid:      false,
		Snapshot:          rf.persister.ReadRaftState(),
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
	}
	rf.lastApplied = rf.lastIncludedIndex
	rf.applyCh <- applyMsg
	return
}
