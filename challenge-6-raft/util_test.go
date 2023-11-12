package main

import (
	"reflect"
	"testing"
)

func TestShit(t *testing.T) {
	l := []int{1, 2, 3, 4}
	r := l[0:2]
	expected := []int{1, 2}
	if !reflect.DeepEqual(r, expected) {
		t.Errorf("Expected slice to be open ended in go")
	}
}
