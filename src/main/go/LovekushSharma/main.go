package main

import (
	optimisedconcurrent "1brc/LovekushSharma/optimisedConcurrent"
	"fmt"
	"time"
)

func main() {
	strt := time.Now()
	// serial.Serial("C:\\Users\\lovekushs\\Desktop\\1brc\\data\\measurements_1mil.txt")
	// fmt.Printf("\nTime taken for 1 million rows:%v", time.Since(strt))
	// strt = time.Now()
	// serial.Serial("C:\\Users\\lovekushs\\Desktop\\1brc\\data\\measurements_100mil.txt")
	// fmt.Printf("\nTime taken for 100 million rows:%v", time.Since(strt))
	// strt = time.Now()
	// serial.Serial("C:\\Users\\lovekushs\\Desktop\\1brc\\data\\measurements_1bil.txt")
	// fmt.Printf("\nTime taken for 1 billion rows:%v", time.Since(strt))
	optimisedconcurrent.Optimisedconcurrent("C:\\Users\\lovekushs\\Desktop\\1brc\\data\\measurements_1bil.txt")
	fmt.Println("\n", time.Since(strt))
}
