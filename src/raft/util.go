package raft

import (
	"log"
	"os"
)

// Debugging
const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	log.SetFlags(log.Lmicroseconds)
	// 创建、追加、读写，777，所有权限
	f, err := os.OpenFile("log.log", os.O_CREATE|os.O_APPEND|os.O_RDWR, os.ModePerm)
	if err != nil {
		return
	}
	defer func() {
		f.Close()
	}()

	log.SetOutput(f)

	if Debug {
		log.Printf(format, a...)
	}
	return
}
