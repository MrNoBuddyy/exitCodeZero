package main

import "fmt"

func main() {
	var ptr *int
	fmt.Println("value of ptr is ", ptr)
	var age int = 23
	var ptrr = &age
	fmt.Println("address of age variable:", ptrr)
	fmt.Println("value of ptrr:", *ptrr)
	*ptrr *= 3
	fmt.Println("my future age: ", age)
}
