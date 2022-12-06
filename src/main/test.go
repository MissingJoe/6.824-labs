package main
import "fmt"


func main() {
	m := make(map[string]bool)
	m["a"] = true
	m["b"] = false
	for w := range m {
		fmt.Println(w)
	}
}
