package raft

import "log"
import "fmt"

// Debugging
const Debug = false 

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func PrefixDPrintf(rf *Raft, format string, a ...interface{}) (n int, err error) {
	if Debug {
		fmt.Printf("[peer %d, term %d] ", rf.me, rf.currentTerm)
		log.Printf(format, a...)
	}
	return
}

