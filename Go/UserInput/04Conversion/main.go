package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

func main() {
	fmt.Println("welcome to our pizza app")
	fmt.Println("pls rate our pizza between 1 to 5")
	reader := bufio.NewReader(os.Stdin)
	input, _ := reader.ReadString('\n')
	fmt.Println("thanks for rating: ", input)
	numRating, err := strconv.ParseFloat(strings.TrimSpace(input), 64)
	numRating = numRating + 1
	fmt.Println("Thanks for rating, ", numRating)
	fmt.Println("input is of wrong type ", err)
}
