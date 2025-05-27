package main

import "fmt"

func main() {
	fmt.Println("delete indexs from slices")
	courses := []string{"Java", "python", "nodeJS", "ReactJS", "Scala"}
	courses = append(courses[:3], courses[4:]...) //append reallocates the memory
	fmt.Println("courses", courses)
}
