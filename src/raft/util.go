package raft

import "log"

// Debugging
const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	log.SetFlags(log.Lmicroseconds)
	if Debug {
		log.Printf(format, a...)
	}
	return
}
