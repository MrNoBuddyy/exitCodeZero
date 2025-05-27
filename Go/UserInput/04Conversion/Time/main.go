package main

import (
	"fmt"
	"time"
)

func main() {
	fmt.Println("welcome to timestudy of time")
	presentTime := time.Now()
	fmt.Println("current time is, ", presentTime.Format("01-02-2006 3:04:05 Monday"))
	createdDate := time.Date(2024, time.November, 12, 7, 07, 0, 1, time.Local)
	fmt.Println(createdDate)
	fmt.Println(createdDate.Format("02-01-2001,Monday"))
}
