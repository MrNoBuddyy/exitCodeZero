package main

import (
	"bufio"
	"fmt"
	"os"
)

func main() {
	welcome := "welcome to user input"
	fmt.Println(welcome)
	reader := bufio.NewReader(os.Stdin)
	fmt.Println("Heyy buddy what's your name: ")
	name, _ := reader.ReadString('\n')
	fmt.Println("welcome to the club", name)

}
