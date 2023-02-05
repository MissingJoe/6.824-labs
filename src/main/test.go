package main

import (
	"fmt"
	"time"
)

func b() int {
	go func() {
		for {
			fmt.Println("1111")
		}
	}()
	fmt.Println("2222")
	time.Sleep(5000)
	return 1
}

func main() {
	b()
}
