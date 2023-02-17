package main

import (
	"fmt"
)

func c(a []int) {
	a[0] = 1
}

type a struct {
	ddd []byte
}

func main() {
	b := a{}
	c := make([]byte, 10)
	if b.ddd == nil {
		fmt.Println(111)
	}
	fmt.Println(b.ddd)

	for i := 0; i < 10; i++ {
		c[i] = 1
	}
	b.ddd = c

	fmt.Println(b.ddd)
}
