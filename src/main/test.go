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
	c := make(map[int]string, 1)
	c[0] = "a"
	a, b := c[1]
	fmt.Println(a, b)
}
