package main

func requestVoteRequestToMap(content RequestVoteRequest) map[string]any {
	return map[string]any{
		"type":           "request_vote",
		"candidate_id":   content.candidateId,
		"term":           content.term,
		"last_log_index": content.lastLogIndex,
		"last_log_term":  content.lastLogTerm,
	}
}

func mapToRequestVoteRequest(content map[string]any) RequestVoteRequest {
	return RequestVoteRequest{
		lastLogTerm:  int(content["last_log_term"].(float64)),
		lastLogIndex: int(content["last_log_index"].(float64)),
		term:         int(content["term"].(float64)),
		candidateId:  content["candidate_id"].(string),
	}
}

func appendEntriesRequestToMap(content AppendEntriesRequest) map[string]any {
	return map[string]any{
		"type":                "append_entries",
		"leader_id":           content.leaderId,
		"term":                content.term,
		"leader_commit_index": content.leaderCommitIndex,
		"prev_log_index":      content.prevLogIndex,
		"prev_log_term":       content.prevLogTerm,
		"entries":             content.entries,
	}
}

func mapToAppendEntriesRequest(content map[string]any) AppendEntriesRequest {
	return AppendEntriesRequest{
		entries:           content["entries"].([]LogEntry),
		prevLogTerm:       int(content["prev_log_term"].(float64)),
		prevLogIndex:      int(content["prev_log_index"].(float64)),
		leaderCommitIndex: int(content["leader_commit_index"].(float64)),
		term:              int(content["term"].(float64)),
		leaderId:          content["leader_id"].(string),
	}
}

func requestVoteResponseToMap(content RequestVoteResponse) map[string]any {
	return map[string]any{
		"type":         "request_vote_ok",
		"term":         content.term,
		"vote_granted": content.voteGranted,
	}
}

func mapToRequestVoteResponse(content map[string]any) RequestVoteResponse {
	return RequestVoteResponse{
		voteGranted: content["vote_granted"].(bool),
		term:        int(content["term"].(float64)),
	}
}

func appendEntriesResponseToMap(content AppendEntriesResponse) map[string]any {
	return map[string]any{
		"type":                      "append_entries_ok",
		"term":                      content.term,
		"success":                   content.success,
		"followerLastCommitedIndex": content.followerLastCommitedIndex,
		"follower_prev_log_index":   content.followerPrevLogIndex,
	}
}

func mapToAppendEntriesResponse(content map[string]any) AppendEntriesResponse {
	return AppendEntriesResponse{
		term:                      int(content["term"].(float64)),
		success:                   content["success"].(bool),
		followerLastCommitedIndex: int(content["followerLastCommitedIndex"].(float64)),
		followerPrevLogIndex:      int(content["follower_prev_log_index"].(float64)),
	}
}
