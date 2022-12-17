package main

import (
	"fmt"
)

type Human struct {
	name  string
	age   int
	phone string
}

type Student struct {
	Human  //匿名字段
	school string
	loan   float32
}

func main() {
	var x int
	x = null
	fmt.Println(x)
}

func sqlQuote(x interface{}) string {
	switch x := x.(type) {
	case nil:
		return "null"
	case int, uint:
		return fmt.Sprintf("%d", x)
	case bool:
		if x {
			return "true"
		}

		return "false"
	case string:
		return "string"
	default:
		panic("no match case")
	}
}
