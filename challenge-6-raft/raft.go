package main

import (
	log "github.com/sirupsen/logrus"
	"sync"
	"time"
)

const (
	heartBeatTimeout = time.Duration(50 * time.Millisecond)
	electionTimeout  = time.Duration(500 * time.Millisecond)
)

type State int

const (
	follower State = iota
	candidate
	leader
)

type LogEntry struct {
	term    int `json:"term"`
	command any `json:"command"`
}

type AppendEntriesRequest struct {
	leaderId          string     `json:"leader_id"`
	term              int        `json:"term"`
	leaderCommitIndex int        `json:"leader_commit_index"`
	prevLogIndex      int        `json:"prev_log_index"`
	prevLogTerm       int        `json:"prev_log_term"`
	entries           []LogEntry `json:"entries"`
}

type AppendEntriesResponse struct {
	term    int  `json:"term"`
	success bool `json:"success"`
}

type RequestVoteRequest struct {
	candidateId  string `json:"candidate_id"`
	term         int    `json:"term"`
	lastLogIndex int    `json:"last_log_index"`
	lastLogTerm  int    `json:"last_log_term"`
}

type RequestVoteResponse struct {
	term        int  `json:"term"`
	voteGranted bool `json:"vote_granted"`
}

type Raft struct {
	//implementation specific, not mentioned in ongaro/ousterhout
	nodeId           string
	peers            []string
	state            State
	electionDeadline time.Time
	mut              sync.Mutex
	transport        RaftTransport

	//persistent state on all servers -> we are not handling this failure mode in this test
	currentTerm int
	votedFor    string
	log         []LogEntry

	//volatile state on all servers
	lastKnownLeader string
	commitIndex     int
	lastApplied     int

	//volatile leader state
	nextIndex     []int
	commitedIndex []int
}

func NewRaft(transport RaftTransport) *Raft {
	return &Raft{
		lastKnownLeader:  "",
		votedFor:         "",
		log:              []LogEntry{{term: 0, command: struct{}{}}},
		currentTerm:      1,
		commitIndex:      0,
		lastApplied:      0,
		state:            follower,
		electionDeadline: time.Now(),
		transport:        transport,
		//initialize nextIndex, commitedIndex, nodeID and peers only on jepsen init
	}
}

func (r *Raft) handleRequestVote(req RequestVoteRequest) RequestVoteResponse {
	r.mut.Lock()
	defer r.mut.Unlock()

	resp := RequestVoteResponse{voteGranted: false, term: req.term}

	log.Errorf("received request vote request at node %s in term %d, from node %s in term %d",
		r.nodeId, r.currentTerm, req.candidateId, req.term)

	if req.term < r.currentTerm {
		log.Errorf("node %s in term %d denied vote request from node %s in term %d, due to smaller term",
			r.nodeId, r.currentTerm, req.candidateId, req.term)
		return resp
	}

	if req.term == r.currentTerm && r.state != follower {
		log.Errorf("node %s in term %d denied vote request from node %s in term %d, "+
			"due being at same term and not being a follower",
			r.nodeId, r.currentTerm, req.candidateId, req.term)
		return resp
	}

	r.currentTerm = req.term
	r.state = follower
	r.votedFor = req.candidateId
	r.lastKnownLeader = ""
	r.electionDeadline = time.Now().Add(electionTimeout)
	resp.voteGranted = true
	return resp
}

func (r *Raft) handleAppendEntries(req AppendEntriesRequest) AppendEntriesResponse {
	//lets just send the whole fucking log, why not kekw
	resp := AppendEntriesResponse{term: r.currentTerm, success: false}

	r.mut.Lock()
	defer r.mut.Unlock()

	log.Errorf("Received append entries request at node %s in term %d, from node %s in term %d",
		r.nodeId, r.currentTerm, req.leaderId, req.term)

	if req.term < r.currentTerm {
		log.Errorf("Refused entries request at node %s in term %d, from node %s in term %d, cuz smaller term",
			r.nodeId, r.currentTerm, req.leaderId, req.term)
		return resp
	}

	// if we get an append entries call that matches our term, we know it is the leader for that term
	// otherwise we break the election safety property discussed in $5.2

	r.electionDeadline = time.Now().Add(electionTimeout)

	log.Errorf("Accepted append entries at node %s in term %d, from node %s in term %d. Acknowledging leader",
		r.nodeId, r.currentTerm, req.leaderId, req.term)

	if req.term > r.currentTerm {
		r.currentTerm = req.term
		r.state = follower
		r.votedFor = ""
		r.lastKnownLeader = req.leaderId
		r.electionDeadline = time.Now().Add(electionTimeout)
	}

	resp.success = true
	return resp
}

func (r *Raft) broadcastAppendEntries() {
	r.mut.Lock()
	req := AppendEntriesRequest{
		term:     r.currentTerm,
		leaderId: r.nodeId,
	}
	r.mut.Unlock()

	log.Errorf("Node %s considers itself leader. Broadcasting append entries")
	wg := sync.WaitGroup{}
	wg.Add(len(r.peers) - 1)
	for _, node := range r.peers {
		if node == r.nodeId {
			continue
		}
		go func() {
			defer wg.Done()
			r.transport.appendEntries(node, req)
		}()
	}
	wg.Wait()
}

func (r *Raft) performElection() {
	log.Errorf("Starting election from node %s in term %d", r.nodeId, r.currentTerm)
	r.mut.Lock()
	r.currentTerm += 1
	r.votedFor = r.nodeId
	r.state = candidate
	r.electionDeadline = time.Now().Add(electionTimeout)
	r.mut.Unlock()

	voteCount := 1

	req := RequestVoteRequest{
		term:        r.currentTerm,
		candidateId: r.nodeId,
	}

	wg := sync.WaitGroup{}
	wg.Add(len(r.peers) - 1)
	for _, node := range r.peers {
		if node == r.nodeId {
			continue
		}
		go func() {
			defer wg.Done()
			resp, err := r.transport.requestVote(node, req)
			if err != nil && resp.voteGranted {
				log.Errorf("Node %s received a vote from node %s", r.nodeId, node)
				voteCount += 1
			}
		}()
	}
	wg.Wait()

	r.mut.Lock()
	defer r.mut.Unlock()

	if r.state != candidate {
		log.Errorf("Aborting election from node %s at term %d, expected state to be candidate, it changed",
			r.nodeId, r.currentTerm)
		return
	}

	if voteCount <= len(r.peers)/2 {
		log.Errorf("Aborting election from node %s: expected to receive at least %d votes to become leader, got %d",
			r.nodeId, len(r.peers)/2+1, voteCount)
		return
	}

	log.Errorf("Node %s successfully became leader in term %d",
		r.nodeId, r.currentTerm)
	r.state = leader
	r.lastKnownLeader = r.nodeId
	//asserting dominance kekw
	r.broadcastAppendEntries()
}

func (r *Raft) loop() {
	//not handling closes or sigint on purpose, unnecessary complexity for this exercise
	electionTicker := time.NewTicker(electionTimeout)
	heartBeatTicker := time.NewTicker(heartBeatTimeout)
	go func() {
		for {
			select {
			case <-electionTicker.C:
				if r.state == leader || !time.Now().After(r.electionDeadline) {
					continue
				}
				r.performElection()
			case <-heartBeatTicker.C:
				if r.state != leader {
					continue
				}
				r.broadcastAppendEntries()
			}
		}
	}()
}
