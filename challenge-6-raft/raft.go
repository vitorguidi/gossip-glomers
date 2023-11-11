package raft

import "time"

const (
	electionTimeout = time.Duration(500 * time.Millisecond)
	rpcTimeout      = time.Duration(150 * time.Millisecond)
)

type LogEntry struct {
	term    int
	command any
}

type AppendEntriesRequest struct {
	term              int
	leaderId          int
	leaderCommitIndex int
	prevLogIndex      int
	prevLogTerm       int
	entries           LogEntry
}

type AppendEntriesResponse struct {
	term    int
	success bool
}

type RequestVoteRequest struct {
	term        int
	candidateId int
	lasLogIndex int
	lastLogTerm int
}

type RequestVoteResponse struct {
	term        int
	voteGranted bool
}

type Raft struct {
	commitC  chan<- any //sends commands to state machine
	proposeC <-chan any //receives write requests from clerk
	nodeId   string
	peers    []string
	//persistent state on all servers -> we are not handling this failure mode in this test
	currentTerm int
	votedFor    string
	log         []LogEntry

	//volatile state on all servers
	lastKnownLeader  string
	commitIndex      int
	lastApplied      int
	electionDeadline time.Time

	//volatile leader state
	nextIndex     []int
	commitedIndex []int
}

func NewRaft(nodeId string, commitC chan<- any, proposeC <-chan any, peers []string) *Raft {
	return &Raft{
		commitC:         commitC,
		proposeC:        proposeC,
		lastKnownLeader: "",
		votedFor:        "",
		log:             []LogEntry{{term: 0, command: struct{}{}}},
		currentTerm:     1,
		commitIndex:     0,
		lastApplied:     0,
		nextIndex:       make([]int, len(peers)), //next index to be appended on peers
		commitedIndex:   make([]int, len(peers)), //last commited index in peers
		nodeId:          nodeId,
		peers:           peers,
	}
}
