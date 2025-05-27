package main

import "fmt"

func main() {
	fmt.Println("welcome to maps")
	languages := make(map[string]string)
	languages["CPP"] = "C++"
	languages["GO"] = "GoLang"
	languages["py"] = "Python"
	fmt.Println(languages)
	for key, value := range languages {
		fmt.Printf("The key %v stands for %v in programming world\n", key, value)
	}
	delete(languages, "py")
	fmt.Println(languages)

}
