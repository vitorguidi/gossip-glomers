package main

import (
	"encoding/json"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"log"
	"os"
	"sync"
	"time"
)

/*
https://www.youtube.com/watch?v=77qpCahU3fo&t=298s&ab_channel=MartinKleppmann
gossip protocol, simple
Idea:
	* keep buffer of messages
	* keep broadcasting until all got received
	* select at max neighbourSpreadFactor nodes to gossip
	* once u get broadcast response, update the seen list
	* if message was successfully broadcasted, remove from buffer
*/

const (
	neighbourSpreadFactor = 3
	gossipInterval        = time.Millisecond * 100
	rpcTimeoutInterval    = time.Millisecond * 200
)

type Server struct {
	node               *maelstrom.Node
	allNodes           []string
	messageBuffer      map[float64]struct{}
	messageSeenByNodes map[float64][]string
	receivedMessages   []float64
	messagesMut        sync.Mutex
}

func newServer() *Server {
	var maelstromNode = maelstrom.NewNode()
	server := &Server{node: maelstromNode}
	maelstromNode.Handle("read", server.handleRead)
	maelstromNode.Handle("topology", server.handleTopology)
	maelstromNode.Handle("broadcast", server.handleBroadcast)
	return server
}

func main() {
	server := newServer()
	if err := server.node.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
	// Execute the node's message loop. This will run until STDIN is closed.

}

func (server *Server) handleRead(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	server.messagesMut.Lock()
	defer server.messagesMut.Unlock()
	body["type"] = "read_ok"
	body["messages"] = server.receivedMessages
	// Echo the original message back with the updated message type.
	return server.node.Reply(msg, body)
}

func (server *Server) handleBroadcast(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	server.messagesMut.Lock()
	defer server.messagesMut.Unlock()

	msgId := body["message"].(float64)
	server.receivedMessages = append(server.receivedMessages, msgId)

	body["type"] = "broadcast_ok"
	delete(body, "message")

	return server.node.Reply(msg, body)
}

// not using topology, we can assume that all nodes are connected and work from this
// as per problem description https://fly.io/dist-sys/3a/
func (server *Server) handleTopology(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	return server.node.Reply(msg, map[string]any{"type": "topology_ok"})
}

func setComplement(seenNodes []string, allNodes []string) {
	//saudade dos streams de java =(
}

func pickAtMostNAtRandom(candidates []string, n int) []string {
	return nil
}
