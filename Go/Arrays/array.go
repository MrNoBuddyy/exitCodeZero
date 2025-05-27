package main

import "fmt"

func main() {
	var myList [4]string

	myList[0] = "a"
	myList[1] = "b"
	myList[2] = "c"
	myList[3] = "a"
	fmt.Println("my list is here ", myList)
	var newList = [4]string{"aa", "vv", "cc", "dd"}
	fmt.Printf("type of myList is %T\n ", myList)
	fmt.Println("my new list is here also", newList)
}
