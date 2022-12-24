package main

import (
	"fmt"
	"sync"
	"time"
)

type fuc struct {
	mu  sync.Mutex
	num int
}

func (f *fuc) b() {
	f.num++
	fmt.Println("aaaa")
}

func (f *fuc) a() {
	var m sync.Mutex
	m.Lock()
	defer m.Unlock()
	go f.b()
	f.num++
	fmt.Println("bbbb")
}

func main() {
	a := time.Now()
	time.Sleep(500 * time.Millisecond)
	if time.Now().Sub(a) > time.Duration(500*time.Millisecond) {
		fmt.Println("1")
	}
}
