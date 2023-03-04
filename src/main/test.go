package main

import (
	"fmt"
)

type sss struct {
	gg map[int]int
	a  [10]int
}

func main() {
	c := sss{}
	c.gg = make(map[int]int)
	c.a[1] = 1
	fmt.Println(c.a)
	b := c
	b.a[1] = 2
	fmt.Println(b.a)
	a := make(map[int][]int, 1)
	a[1] = append(a[1], 1)
	fmt.Println(a)
}
