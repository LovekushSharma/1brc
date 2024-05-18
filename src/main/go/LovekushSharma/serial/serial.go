package serial

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
)

type stats struct {
	min, max, sum float64
	count         int64
}

func Serial(filePath string) {
	file, err := os.Open(filePath)
	if err != nil {
		log.Fatalln(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	tmpdata := make(map[string]stats)
	for scanner.Scan() {
		str := scanner.Text()
		slst := strings.Split(str, ";")
		val, err := strconv.ParseFloat(slst[1], 64)
		if err != nil {
			continue
		}
		_, exists := tmpdata[slst[0]]
		if !exists {
			tmpdata[slst[0]] = stats{min: val, max: val, sum: val, count: 1}
		} else {
			tmpdata[slst[0]] = stats{
				min:   min(tmpdata[slst[0]].min, val),
				max:   max(tmpdata[slst[0]].max, val),
				sum:   tmpdata[slst[0]].sum + val,
				count: tmpdata[slst[0]].count + 1,
			}
		}
	}
	for k, v := range tmpdata {
		fmt.Printf("<%s;%.2f/%.2f/%.2f>", k, v.min, v.max, v.sum/float64(v.count))
	}
}
