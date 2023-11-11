#!/bin/zsh
maelstrom test -w lin-kv --bin /Users/guidi/projects/gossip-glomers/challenge-6-raft/raft --node-count 1 \
 --concurrency 2n --time-limit 20 --rate 10
