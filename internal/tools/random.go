package tools

import (
	"math/rand"
	"time"
)

var r = rand.New(rand.NewSource(time.Now().UnixNano()))

func RandInt(min, max int) int {
	if min > max {
		min, max = max, min
	}
	return r.Intn(max-min+1) + min
}

func RandDuration(min, max int) time.Duration {
	n := RandInt(min, max)

	return time.Duration(n) * time.Millisecond
}
