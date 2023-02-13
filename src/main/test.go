package main

import (
	"fmt"
	"time"
)

func c() {
	for {
		fmt.Println("1111")
		time.Sleep(100 * time.Millisecond)
	}
}

func b() {
	go c()
	fmt.Println("2222")
}

func main() {
	b()
	fmt.Println("finished all")
	time.Sleep(1 * time.Second)
}
