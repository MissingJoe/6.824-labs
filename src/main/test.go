package main

import (
	"fmt"
)

type c struct {
	a int
	b int
}

func f(a *int, b *int) {
	*b = *a
}

func main() {
	h := c{a: 1}

	fmt.Println(h.b)
}
