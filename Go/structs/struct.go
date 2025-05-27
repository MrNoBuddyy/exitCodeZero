package main

import "fmt"

func main() {
	vijay := User{
		"Vijay", true, 23, "+91 8239"}
	fmt.Println(vijay)
	fmt.Printf("The user details are %+v\n", vijay)
}

type User struct {
	Name    string
	Status  bool
	Age     int
	Contact string
}
