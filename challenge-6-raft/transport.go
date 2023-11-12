package main

import (
	"context"
	"encoding/json"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	log "github.com/sirupsen/logrus"
	"time"
)

const rpcTimeout = time.Duration(150 * time.Millisecond)

type RaftTransport interface {
	appendEntries(dest string, content AppendEntriesRequest) (AppendEntriesResponse, error)
	requestVote(dest string, content RequestVoteRequest) (RequestVoteResponse, error)
}

type MaelstromTransport struct {
	node *maelstrom.Node
}

func (t *MaelstromTransport) appendEntries(dest string, content AppendEntriesRequest) (AppendEntriesResponse, error) {
	ctx, _ := context.WithTimeout(context.Background(), rpcTimeout)
	replyMsg, err := t.node.SyncRPC(ctx, dest, content)
	if err != nil {
		log.Errorf("Failed to sync send append entries rpc from %s to %s in raft maelstrom transport: %s",
			t.node.ID(), dest, err)
		return AppendEntriesResponse{}, err
	}
	replyBody := replyMsg.Body
	var resp AppendEntriesResponse
	if err = json.Unmarshal(replyBody, &resp); err != nil {
		log.Errorf("Failed to deserialize append entries response in raft maelstrom transport: %s", err)
		return AppendEntriesResponse{}, err
	}
	return resp, nil
}

func (t *MaelstromTransport) requestVote(dest string, content RequestVoteRequest) (RequestVoteResponse, error) {
	ctx, _ := context.WithTimeout(context.Background(), rpcTimeout)
	replyMsg, err := t.node.SyncRPC(ctx, dest, content)
	if err != nil {
		log.Errorf("Failed to sync send request vote rpc from %s to %s in raft maelstrom transport: %s",
			t.node.ID(), dest, err)
		return RequestVoteResponse{}, err
	}
	replyBody := replyMsg.Body
	var resp RequestVoteResponse
	if err = json.Unmarshal(replyBody, &resp); err != nil {
		log.Errorf("Failed to deserialize request vote response in raft maelstrom transport")
		return RequestVoteResponse{}, err
	}
	return resp, nil
}
