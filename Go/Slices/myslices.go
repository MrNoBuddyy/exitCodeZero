package slices

import (
	"fmt"
	"sort"
)

func main() {
	fmt.Println("hello and welcome to slices ")
	var fruitList = []string{}
	fmt.Printf("type of fruitlist is %T\n ", fruitList)
	// trpe of fruitlist is []string  this is what exactly slice is
	fruitList = []string{"Apple", "Mango", "PineApple"}
	fmt.Println(fruitList)
	fruitList = append(fruitList, "Banana", "BlackBerry")
	fmt.Println(fruitList)
	fmt.Println(append(fruitList[2:]))
	fmt.Println(append(fruitList[:3]))
	highScores := make([]int, 4)
	highScores[0] = 3
	highScores[1] = 9
	highScores[2] = 4
	highScores[3] = 5
	fmt.Println(highScores)
	fmt.Printf("type of highScores is %T\n ", highScores)
	highScores = append(highScores, 33, 444, 55) //as we do this append it re allocates the memory to the full silce
	fmt.Println(sort.IntsAreSorted(highScores))
	sort.Ints(highScores)
	fmt.Println(highScores)
	fmt.Println(sort.IntsAreSorted(highScores))

}
