package main

import (
	"reflect"
	"testing"
)

func TestGetNodeIdFromKey(t *testing.T) {

	type ConversionResult struct {
		expectedResult string
		nodeTotal      int
		key            string
	}

	errorCases := []ConversionResult{
		{expectedResult: "", nodeTotal: 10, key: "nonsense"},
		{expectedResult: "", nodeTotal: 10, key: "bullshit"},
	}

	t.Run("Should not be able to convert when key does not conform to standard", func(t *testing.T) {
		for _, testCase := range errorCases {
			_, err := GetNodeIdFromKey(testCase.key, testCase.nodeTotal)
			AssertError(t, err, testCase.key)
		}
	})

	okCases := []ConversionResult{
		{expectedResult: "n3", nodeTotal: 10, key: "3"},
		{expectedResult: "n2", nodeTotal: 10, key: "12"},
		{expectedResult: "n0", nodeTotal: 10, key: "10"},
	}

	t.Run("Should  be able to convert valid keys", func(t *testing.T) {
		for _, testCase := range okCases {
			actualResult, err := GetNodeIdFromKey(testCase.key, testCase.nodeTotal)
			AssertNoError(t, err)
			AssertCorrectNode(t, testCase.expectedResult, actualResult)
		}
	})

}

func AssertError(t testing.TB, err error, key string) {
	t.Helper()
	if err == nil {
		t.Errorf("Expected error converting invalid key %s", key)
	}
}

func AssertNoError(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Errorf("Expected no error converting invalid key, got: %s", err)
	}
}

func AssertCorrectNode(t testing.TB, expectedNode string, actualNode string) {
	if expectedNode != actualNode {
		t.Errorf("Expected resulting node to be %s, got %s", expectedNode, actualNode)
	}
}

func TestSplitDataByNodeId(t *testing.T) {
	type TestCase struct {
		input          map[string]int
		totalNodes     int
		expectedOutput map[string]map[string]int
	}

	testCases := []TestCase{
		{
			input:      map[string]int{"s0": 10, "s1": 12, "s2": 13},
			totalNodes: 2,
			expectedOutput: map[string]map[string]int{
				"n0": {
					"s0": 10,
					"s2": 13,
				},
				"n1": {
					"s1": 12,
				},
			},
		},
		{
			input:      map[string]int{"s0": 10, "s1": 12, "s2": 13},
			totalNodes: 1,
			expectedOutput: map[string]map[string]int{
				"n0": {
					"s0": 10,
					"s2": 13,
					"s1": 12,
				},
			},
		},
	}

	for _, testCase := range testCases {
		output := SplitDataByNodeId(testCase.input, testCase.totalNodes)
		if !reflect.DeepEqual(testCase.expectedOutput, output) {
			t.Error("Expected maps to be equal, they are not")
		}
	}

}
