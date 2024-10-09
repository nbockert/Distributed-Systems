package raft

//
// This is an outline of the API that raft must expose to
// the service (or tester). See comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   Create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   Start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   Each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester) in the same server.

//put candidate in for loop

import (
	"bytes"
	"cs350/labgob"
	"cs350/labrpc"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// As each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). Set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}
type LogEntry struct {
	Term    int
	Command interface{}
}

type AppendEntries struct {
	Term         int //leaders term
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry //or is it the LogEntry struct?
	LeaderCommit int
}
type AppendEntriesRep struct {
	Term    int
	Success bool
	//Match   int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu          sync.Mutex          // Lock to protect shared access to this peer's state
	peers       []*labrpc.ClientEnd // RPC end points of all peers
	persister   *Persister          // Object to hold this peer's persisted state
	me          int                 // This peer's index into peers[]
	dead        int32               // Set by Kill()
	currentTerm int
	votedFor    int
	log         []LogEntry
	commitIndex int
	lastApplied int
	nextIndex   []int //index of the next entry that leader needs to replicate for that particular server
	matchIndex  []int //what is match index and what do we initialize it too
	state       int
	Lastheard   time.Time
	Votes       int

	ApplyCh chan ApplyMsg
}

// Return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	rf.mu.Lock()

	state := rf.state
	terms := rf.currentTerm
	me := rf.me
	rf.mu.Unlock()

	if state == 2 {
		isleader = true
		term = terms
		//rf.persist()
		fmt.Println("Leader at GetState is", me)
		//fmt.Println("server:", rf.me, "'s term is", term)

	} else {
		isleader = false
		term = terms
		//fmt.Println("server:", rf.me, "'s term is", term)
		//rf.persist()
	}

	// Your code here (4A).
	//everytime you recive a vote keep a count in get state and then when it is majority you tell it that its the leader

	return term, isleader
}

// Save Raft's persistent state to stable storage, where it
// can later be retrieved after a crash and restart. See paper's
// Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (4B).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)

}

// Restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state? - what the hell does this mean did I write that?
		return
	}
	// Your code here (4B).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		panic("Error decoding persistent state")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
}

// Example RequestVote RPC arguments structure.
// Field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (4A, 4B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// Example RequestVote RPC reply structure.
// Field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (4A).
	Term        int
	VoteGranted bool
}

// Example RequestVote RPC handler.
// In request vote put who they voted for in peers
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (4A, 4B).

	//check if its log is at least as up to date for 4b
	rf.mu.Lock()
	//fmt.Println("making it to requestvote")
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false

		rf.mu.Unlock()
		return
	}
	if args.LastLogTerm < rf.log[len(rf.log)-1].Term ||
		(args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex < len(rf.log)-1) {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm

		rf.mu.Unlock()
		return
		//fmt.Println(rf.me, "voted for", rf.votedFor)
	} else if rf.votedFor == args.CandidateId || rf.votedFor == -1 && args.Term >= rf.currentTerm && rf.log[len(rf.log)-1].Term <= args.LastLogTerm {
		fmt.Println("candidate is", args.CandidateId, " server is ", rf.me)
		//fmt.Println(rf.me, "voted for", rf.votedFor)
		rf.votedFor = args.CandidateId
		reply.Term = args.Term
		rf.state = 1
		rf.currentTerm = args.Term
		reply.VoteGranted = true

		rf.persist()
		rf.mu.Unlock()
		return

	} else {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}

}

// The service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. If this
// server isn't the leader, returns false. Otherwise start the
// agreement and return immediately. There is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. Even if the Raft instance has been killed,
// this function should return gracefully.
//
// The first return value is the index that the command will appear at
// if it's ever committed. The second return value is the current
// term. The third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true
	// Your code here (4B).
	_, isLeader = rf.GetState()
	if !isLeader {
		rf.mu.Lock()
		index = 0 //or should this be 0
		term = rf.currentTerm
		rf.mu.Unlock()
	} else {
		//fmt.Println("in start", rf.me)
		rf.mu.Lock()

		//just call sendenteries
		term = rf.currentTerm
		logentry := LogEntry{
			Term:    rf.currentTerm,
			Command: command,
		}
		rf.log = append(rf.log, logentry)
		index = len(rf.log) - 1 //do I do -1
		rf.persist()
		rf.mu.Unlock()
		//go rf.SendEntries()

	}
	return index, term, isLeader
}

// The tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. Your code can use killed() to
// check whether Kill() has been called. The use of atomic avoids the
// need for a lock.
//
// The issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. Any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) SendVotes() {
	rf.mu.Lock()
	me := rf.me
	peers := len(rf.peers)
	rf.mu.Unlock()

	for i := 0; i < peers; i++ {
		var last int
		var in int

		if i != me {

			go func(peerIndex int) {
				//defer l.Done()
				rf.mu.Lock()

				fmt.Println("sendvotes server", rf.me, "log size is", len(rf.log), "peer", peerIndex)
				//fmt.Println(rf.log)
				in = len(rf.log) - 1
				last = rf.log[in].Term
				term := rf.currentTerm
				state := rf.state

				args := RequestVoteArgs{
					Term:         term,
					CandidateId:  me,
					LastLogIndex: in,
					LastLogTerm:  last,
				}
				rf.mu.Unlock()
				reply := RequestVoteReply{}

				if state == 0 {
					ok := rf.peers[peerIndex].Call("Raft.RequestVote", &args, &reply)

					if ok {
						rf.mu.Lock()
						if reply.VoteGranted {
							rf.Votes++
							if rf.Votes >= len(rf.peers)/2+1 {
								rf.state = 2

								rf.persist()
								rf.mu.Unlock()
								return
							} else { //changed this
								rf.mu.Unlock()
							}

						} else {
							fmt.Println(rf.me, "is a follower because someone did not vote for it")

							if reply.Term > rf.currentTerm {
								rf.currentTerm = reply.Term
							}
							rf.votedFor = -1
							rf.state = 1
							rf.persist()
							rf.mu.Unlock()
							return
						}
					}

				}

			}(i)
			time.Sleep(10 * time.Millisecond)

		}

	}
}

func (rf *Raft) ticker() {

	//election timer

	for !rf.killed() {
		rf.mu.Lock()
		if rf.state == 2 {
			if rf.state == 2 && len(rf.log) > 1 {
				if rf.nextIndex == nil {
					for i := 0; i < len(rf.peers); i++ {
						rf.nextIndex = append(rf.nextIndex, len(rf.log)) //changed this from +1 lmk if it messes up lol
						rf.matchIndex = append(rf.matchIndex, 0)
					}

				}
				rf.mu.Unlock()
				rf.SendEntries()
				fmt.Println("the leader is here")

			} else {
				rf.mu.Unlock()
				go rf.Heartbeat()
			}
			time.Sleep(40 * time.Millisecond)
		} else {

			timeout := time.Duration(rand.Intn(600)+500) * time.Millisecond
			rf.Lastheard = time.Now()
			Lastheard := rf.Lastheard
			rf.mu.Unlock()
			for time.Since(Lastheard) <= timeout {
				rf.mu.Lock()
				if rf.state == 0 {
					rf.state = 1
				}
				if rf.state == 1 {
					Lastheard = rf.Lastheard
					//rf.votedFor = -1
					rf.Votes = 0

					rf.mu.Unlock()
				} else {
					rf.mu.Unlock()
				}

				time.Sleep(10 * time.Millisecond)

			}
			rf.mu.Lock()

			rf.state = 0

			//do something so that if there is a heartbeat sent by the leader this gets reset as a follower
			if time.Since(rf.Lastheard) > timeout {
				if rf.state == 0 { //why would I put the candidate here
					rf.currentTerm += 1
					fmt.Println(rf.me, "timed out with new term", rf.currentTerm)
					rf.votedFor = rf.me
					rf.state = 0
					rf.Votes = 1
					rf.persist()
					if time.Since(rf.Lastheard) > timeout {
						rf.mu.Unlock()
						rf.SendVotes()
					} else {
						rf.state = 1
						rf.votedFor = -1
						rf.persist()
						rf.mu.Unlock()
					}
				} else {
					rf.mu.Unlock()
				}
			} else {
				rf.state = 1
				rf.persist()
				rf.mu.Unlock()
			}
			//time.Sleep(10 * time.Millisecond)
		}

	}

}

func (rf *Raft) Heartbeat() {

	rf.mu.Lock()
	timer := time.Duration(rand.Intn(300)) * time.Millisecond //changed this
	peers := len(rf.peers)
	me := rf.me
	rf.mu.Unlock()

	for j := 0; j < peers; j++ {
		rf.mu.Lock()
		term := rf.currentTerm
		state := rf.state
		rf.mu.Unlock()
		if me != j && state == 2 {

			go func(peerIndex int) {

				args := AppendEntries{
					Term:         term,
					LeaderId:     me,
					PrevLogIndex: -1,
				}
				reply := AppendEntriesRep{}
				//fmt.Println("RPC call at heartbeats for", me, "from", peerIndex)
				ok := rf.peers[peerIndex].Call("Raft.AppendEntries", &args, &reply)
				if ok {
					//fmt.Println("RPC call at heartbeats for", rf.me, "from", peerIndex, "reply was", reply.Success)
					//fmt.Println("log size at heartbeats is", len(rf.log))
					if reply.Success {
						//rf.heard = true
						return
					} else if !reply.Success {
						//rf.heard = false
						rf.mu.Lock()
						rf.state = 1
						rf.currentTerm = reply.Term
						rf.votedFor = -1 // don't get rid of this
						fmt.Println(rf.me, "is a follower at heartbeats")
						rf.persist()
						rf.mu.Unlock()
						return
					} else {
						return
					}
				}

			}(j)

		}
		time.Sleep(timer)

	}

}
func (rf *Raft) SendEntries() {
	rf.mu.Lock()
	//fmt.Println("sending append enteries after commit, leader is ", rf.me)
	peers := rf.peers

	rf.mu.Unlock()
	//var w sync.WaitGroup
	for j := 0; j < len(peers); j++ {
		rf.mu.Lock()
		length := len(rf.log)
		next := rf.nextIndex[j]
		term := rf.currentTerm
		me := rf.me
		rf.mu.Unlock()
		if next <= length+1 {
			if me != j && length > 1 {

				go func(peerIndex int) {
					var start int
					var prevLogIndex int
					var entries []LogEntry
					var prevterm int
					var commit int

					if length-1 >= next {
						rf.mu.Lock()
						//fmt.Println(rf.me, rf.nextIndex, rf.matchIndex)
						start = rf.nextIndex[peerIndex]
						entries = rf.log[start:]
						commit = rf.commitIndex
						rf.mu.Unlock()
					} else {
						rf.mu.Lock()
						start = rf.matchIndex[peerIndex] + 1
						entries = rf.log[start:]
						commit = rf.commitIndex
						rf.mu.Unlock()
					}
					if start != 0 {
						rf.mu.Lock()
						prevLogIndex = start - 1
						prevterm = rf.log[prevLogIndex].Term
						rf.mu.Unlock()
					} else {
						rf.mu.Lock()
						prevLogIndex = 0
						prevterm = rf.log[prevLogIndex].Term
						rf.mu.Unlock()
					}

					args := AppendEntries{
						Term:         term,
						LeaderId:     me,
						Entries:      entries,      //this is failing RPC test I'm going to kill my self omg
						PrevLogIndex: prevLogIndex, //made it next index -2 lmk if this causes errors OR SHOULD IT BE MATCHINDEX +1?
						PrevLogTerm:  prevterm,     //check to see if this is right - do you do -1?
						LeaderCommit: commit,
					}
					fmt.Println(peerIndex, "the log before sending is:", entries)

					reply := AppendEntriesRep{}
					ok := rf.peers[peerIndex].Call("Raft.AppendEntries", &args, &reply)
					time.Sleep(10 * time.Millisecond)
					if ok {
						if reply.Term > term {
							rf.mu.Lock()
							//fmt.Println("The term of the reply", reply.Term, "is greater than current term", rf.currentTerm, "so I am setting leader to follower (debug)")
							rf.state = 1
							rf.votedFor = -1
							rf.currentTerm = reply.Term
							//rf.heard = false
							rf.persist()
							rf.mu.Unlock()
							return
						}

						if args.Term != term {
							return // Drop old RPC replies
						}

						//fmt.Println("RPC call at sendEnteries for", rf.me, "from", peerIndex, "reply was", reply.Success)
						if reply.Success {
							rf.mu.Lock()

							rf.nextIndex[peerIndex] = args.PrevLogIndex + 1 + len(args.Entries)
							rf.matchIndex[peerIndex] = args.PrevLogIndex + len(args.Entries)
							rf.mu.Unlock()
							go rf.Majority()

						} else {
							rf.mu.Lock()
							//rf.matchIndex[peerIndex] = reply.Match
							rf.nextIndex[peerIndex] = 1 //optimize by conflict index so that you can send it to where it conflicts
							fmt.Println(peerIndex, "is sent after another AppendEnteries after failing")
							//fmt.Println("next index=", rf.nextIndex, "matchindex", rf.matchIndex)
							//fmt.Println(rf.nextIndex[peerIndex])
							//idk if this will cause errors.
							rf.mu.Unlock()
						}
					}

				}(j)
				time.Sleep(30 * time.Millisecond)

			}

		} else {
			rf.mu.Lock()
			//fmt.Println("had to decrement nextindex because it was greater than log+1")
			rf.nextIndex[j] = 1
			rf.mu.Unlock()
		}

	}

}
func (rf *Raft) Majority() {
	rf.mu.Lock()
	fmt.Println("match index before it finds majority ", rf.matchIndex)
	fmt.Println("commit index before it finds majority ", rf.commitIndex)

	newmajority := rf.commitIndex
	for i := rf.commitIndex + 1; i <= len(rf.log); i++ {
		num := 1
		for server := range rf.peers {
			if server != rf.me && rf.matchIndex[server] >= i { //&& rf.log[i].Term == rf.currentTerm
				num++
				if num == len(rf.peers)/2+1 {
					newmajority = i
					break
				}
			}
		}
	}

	//DPrintf("new commit index %d, old: %d", newCommitIndex, rf.commitIndex)
	//fmt.Println("new majority", newmajority)
	if newmajority != rf.commitIndex {
		rf.commitIndex = newmajority
		fmt.Println("commitIndex is now ", rf.commitIndex)
		rf.mu.Unlock()
		//fmt.Println("calling ApplySend")
		go rf.ApplySend()
	} else {
		rf.mu.Unlock()
	}

}
func (rf *Raft) ApplySend() {
	rf.mu.Lock()
	fmt.Println("sending message for", rf.me, " last applied is ", rf.lastApplied, " and commit index is", rf.commitIndex)
	for rf.lastApplied < rf.commitIndex { //have to have this so all servers see it
		msg := ApplyMsg{
			CommandValid: true, //when is commandvalid false
			Command:      rf.log[rf.lastApplied+1].Command,
			CommandIndex: rf.lastApplied + 1,
		}
		rf.mu.Unlock()
		rf.ApplyCh <- msg
		rf.mu.Lock()
		rf.lastApplied++

	}
	rf.mu.Unlock()
	//fmt.Println("released lock for", rf.me)

}

// fix this omgggggg
func (rf *Raft) AppendEntries(arg *AppendEntries, reply *AppendEntriesRep) {
	//fmt.Println("in append entries")
	if arg.Entries == nil { //do this for leader election

		_, leader := rf.GetState()

		rf.mu.Lock()

		//rf.heartchan <- true

		if arg.Term < rf.currentTerm {
			//rf.heard = false
			reply.Success = false
			reply.Term = rf.currentTerm

			rf.mu.Unlock()
			return
			//rf.votedFor = -1 //passes autograder when I have this line even though it is incorrect but whatever

		}
		rf.Lastheard = time.Now()
		if leader && rf.currentTerm <= arg.Term {
			fmt.Println(rf.me, "was made into a follower")
			rf.votedFor = -1
			rf.state = 1
			rf.currentTerm = arg.Term
		}

		if arg.PrevLogIndex > -1 {
			reply.Success = true
			reply.Term = rf.currentTerm

			if arg.LeaderCommit > rf.commitIndex {
				fmt.Println("empty log but new majority")
				if arg.LeaderCommit > len(rf.log)-1 {
					rf.commitIndex = len(rf.log) - 1
				} else {
					rf.commitIndex = arg.LeaderCommit

				}
				rf.persist()
				rf.mu.Unlock()
				go rf.ApplySend()
			} else if rf.lastApplied < rf.commitIndex {
				rf.mu.Unlock()
				go rf.ApplySend()
			} else {
				rf.mu.Unlock()
			}

			return
		}
		if arg.Term == rf.currentTerm {
			rf.state = 1

			reply.Success = true
			reply.Term = rf.currentTerm

		}
		if arg.Term > rf.currentTerm {
			reply.Term = arg.Term
			rf.currentTerm = arg.Term

			reply.Success = true
			//rf.votedFor = -1
			rf.state = 1

		}
		rf.mu.Unlock()

	} else if arg.Entries != nil {
		_, leader := rf.GetState()

		rf.mu.Lock()
		if leader && rf.currentTerm <= arg.Term {
			fmt.Println(rf.me, "was made into a follower")
			rf.votedFor = -1
			rf.state = 1
		}
		fmt.Println("In AppendEntries, log size is", len(rf.log), "and prevlogindex is", arg.PrevLogIndex)
		fmt.Println(rf.me, "'s log:", rf.log)
		if arg.Term < rf.currentTerm { //&& len(rf.log)-1 > len(arg.Entries)+arg.PrevLogIndex
			fmt.Println("leaders term is less than servers term")
			reply.Term = rf.currentTerm
			reply.Success = false
			rf.mu.Unlock()
			return

		} else if len(rf.log)-1 < arg.PrevLogIndex {
			fmt.Println("log is too short need to decrement nextindex")
			reply.Term = arg.Term
			reply.Success = false
			rf.mu.Unlock()
			return

		}
		rf.Lastheard = time.Now()

		if rf.log[arg.PrevLogIndex].Term == arg.PrevLogTerm && rf.log[len(rf.log)-1].Term <= arg.Entries[len(arg.Entries)-1].Term {
			fmt.Println("success at appendenteries!")
			rf.log = rf.log[:arg.PrevLogIndex+1] //this might cause mad errors
			if len(rf.log)-1 < arg.PrevLogIndex+len(arg.Entries) {
				for i := 0; i < len(arg.Entries); i++ {
					reply.Term = rf.currentTerm
					reply.Success = true
					logentry := LogEntry{
						Term:    arg.Entries[i].Term,
						Command: arg.Entries[i].Command,
					}
					rf.log = append(rf.log, logentry)

				}
			}
			fmt.Println(rf.me, "'s log", rf.log)
			reply.Success = true
			reply.Term = rf.currentTerm
			if arg.LeaderCommit > rf.commitIndex {
				if arg.LeaderCommit > len(rf.log)-1 {
					rf.commitIndex = len(rf.log) - 1
				} else {
					rf.commitIndex = arg.LeaderCommit
				}
				// if rf.commitIndex-rf.lastApplied > 1 {
				// 	rf.commitIndex = rf.lastApplied + 1
				// }
				rf.persist()
				rf.mu.Unlock()
				go rf.ApplySend()
			} else if rf.lastApplied < rf.commitIndex {
				// if rf.commitIndex-rf.lastApplied > 1 {
				// 	rf.commitIndex = rf.lastApplied + 1
				// }
				rf.mu.Unlock()
				go rf.ApplySend()
			} else {
				rf.mu.Unlock()
			}
			return

			//fmt.Println("after replication log size is", len(rf.log))

		} else {
			//fmt.Println("is it ever making it here?")
			if rf.log[arg.PrevLogIndex].Term < arg.PrevLogTerm {
				rf.log = rf.log[:arg.PrevLogIndex+1]
			}
			reply.Term = rf.currentTerm
			// if len(rf.log) > 0 {
			// 	reply.Match = len(rf.log) - 1
			// } else {
			// 	reply.Match = 0
			// }

			reply.Success = false
			rf.mu.Unlock()
			//this is allowing 1 to win 2's vote
		}

	}
	time.Sleep(100 * time.Millisecond)
}

// The service or tester wants to create a Raft server. The ports
// of all the Raft servers (including this one) are in peers[]. This
// server's port is peers[me]. All the servers' peers[] arrays
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
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = 1 //make this start as a follower

	rf.ApplyCh = applyCh //committed commands to tester
	rf.commitIndex = 0
	rf.lastApplied = 0
	logent := LogEntry{
		Term:    rf.currentTerm,
		Command: nil,
	}
	rf.log = append(rf.log, logent)
	// for i := 0; i < len(rf.peers); i++ {
	// 	rf.matchIndex = append(rf.matchIndex, 0)
	// }

	rf.readPersist(persister.ReadRaftState())
	rf.persist()

	// start ticker goroutine to start elections.
	go rf.ticker()

	return rf
}
