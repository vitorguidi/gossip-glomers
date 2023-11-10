package main

import (
	"fmt"
	"strconv"
)

var expectedKeyPattern = "^s(0|[1-9][0-9]*)$"

func GetNodeIdFromKey(key string, nodeTotal int) (string, error) {
	val, err := strconv.Atoi(key)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("n%d", val%nodeTotal), nil
}

func SplitDataByNodeId[T any](data map[string]T, totalServers int) map[string]map[string]T {
	ans := make(map[string]map[string]T)
	for key, value := range data {
		nodeId, err := GetNodeIdFromKey(key, totalServers)
		if err != nil {
			panic("expected to be able to convert key to target node")
		}
		_, found := ans[nodeId]
		if !found {
			ans[nodeId] = make(map[string]T)
		}
		ans[nodeId][key] = value
	}
	return ans
}
