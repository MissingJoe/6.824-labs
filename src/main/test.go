package main

import (
	//"fmt"
	"fmt"
	"sync"
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
	c := fuc{
		num: 0,
	}
	c.a()
}
