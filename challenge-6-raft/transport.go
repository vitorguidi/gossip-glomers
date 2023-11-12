package main

import (
	"context"
	"encoding/json"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	log "github.com/sirupsen/logrus"
	"time"
)

const rpcTimeout = 200 * time.Millisecond

type RaftTransport interface {
	appendEntries(dest string, content AppendEntriesRequest) (AppendEntriesResponse, error)
	requestVote(dest string, content RequestVoteRequest) (RequestVoteResponse, error)
}

type MaelstromTransport struct {
	node *maelstrom.Node
}

func (t *MaelstromTransport) appendEntries(dest string, content AppendEntriesRequest) (AppendEntriesResponse, error) {
	ctx, _ := context.WithTimeout(context.Background(), rpcTimeout)
	reqBody := appendEntriesRequestToMap(content)

	log.Printf("Sending append entries message from node %s to node %s, content: %s",
		t.node.ID(), dest, content)
	replyMsg, err := t.node.SyncRPC(ctx, dest, reqBody)

	if err != nil {
		log.Errorf("Failed to sync send append entries rpc from %s to %s in raft maelstrom transport: %s",
			t.node.ID(), dest, err)
		return AppendEntriesResponse{}, err
	}

	replyBody := replyMsg.Body
	var resp map[string]any
	if err = json.Unmarshal(replyBody, &resp); err != nil {
		log.Errorf("Failed to deserialize append entries response in raft maelstrom transport: %s", err)
		return AppendEntriesResponse{}, err
	}

	//log.Errorf("Successfully got append entries response in node %s from %s in raft maelstrom transport: %s",
	//	t.node.ID(), dest, resp)
	return mapToAppendEntriesResponse(resp), nil
}

func (t *MaelstromTransport) requestVote(dest string, content RequestVoteRequest) (RequestVoteResponse, error) {
	ctx, _ := context.WithTimeout(context.Background(), rpcTimeout)
	reqBody := requestVoteRequestToMap(content)

	log.Printf("Sending request vote message from node %s to node %s, content: %s",
		t.node.ID(), dest, reqBody)
	replyMsg, err := t.node.SyncRPC(ctx, dest, reqBody)

	if err != nil {
		log.Errorf("Failed to sync send request vote rpc from %s to %s in raft maelstrom transport: %s",
			t.node.ID(), dest, err)
		return RequestVoteResponse{}, err
	}

	replyBody := replyMsg.Body
	var resp map[string]any
	if err = json.Unmarshal(replyBody, &resp); err != nil {
		log.Errorf("Failed to deserialize request vote response in raft maelstrom transport")
		return RequestVoteResponse{}, err
	}

	log.Errorf("Successfully got request vote response in node %s from %s in raft maelstrom transport: %s",
		t.node.ID(), dest, resp)
	return mapToRequestVoteResponse(resp), nil
}
