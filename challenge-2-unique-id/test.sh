#!/bin/zsh
maelstrom test -w unique-ids --bin /Users/guidi/projects/gossip-glomers/challenge-2-unique-id/unique-id \
 --time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition
