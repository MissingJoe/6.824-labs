package main

import "io/ioutil"
import "os"
import "fmt"


func main() {
	ofile, _ := ioutil.TempFile("", "mr-tmp-*")
	c := 1
	d := 0
	fmt.Fprintf(ofile, "%v %v\n", c, d)
	ofile.Close()
	os.Rename(ofile.Name(), "a")

	ofile1, _ := ioutil.TempFile("", "mr-tmp-*")
	c = 3
	d = 2
	fmt.Fprintf(ofile1, "%v %v\n", c, d)
	ofile1.Close()
	os.Rename(ofile1.Name(), "a")
}
