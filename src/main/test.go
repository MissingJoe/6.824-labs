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
	c := make([]int, 1)
	c = append(c, 1)
	fmt.Println(c[0])
}
