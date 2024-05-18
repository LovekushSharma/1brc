package optimisedconcurrent

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
)

type chunk struct {
	chunkSize int
	offset    int64
}

type stats struct {
	min, max, sum float64
	count         int64
}

type psedomap struct {
	key string
	val stats
}

func worker(strChan chan string, mapChan chan psedomap, wg *sync.WaitGroup) {
	defer wg.Done()
	tmpData := make(map[string]stats)
	for str := range strChan {
		lines := strings.Split(str, "\n")
		for _, line := range lines {
			l := strings.Split(line, ";")
			if len(l) < 2 {
				continue
			}
			l[1] = strings.Trim(l[1], "\r")
			v, err := strconv.ParseFloat(l[1], 64)
			if err != nil {
				continue
			}
			_, exists := tmpData[l[0]]
			if exists {
				tmpData[l[0]] = stats{
					min:   min(tmpData[l[0]].min, v),
					max:   max(tmpData[l[0]].max, v),
					sum:   tmpData[l[0]].sum + v,
					count: tmpData[l[0]].count + 1,
				}
			} else {
				tmpData[l[0]] = stats{min: v, max: v, sum: v, count: 1}
			}
		}
		for k, v := range tmpData {
			mapChan <- psedomap{key: k, val: v}
		}
	}
}

func Optimisedconcurrent(filepath string) {
	bufferSize := 1024 * 1024 * 50 //50 mb

	file, err := os.Open(filepath)
	if err != nil {
		panic(err)
	}

	info, err := file.Stat()
	if err != nil {
		panic(err)
	}
	fileSize := info.Size()

	numtreads := fileSize / int64(bufferSize)
	chunks := make([]chunk, numtreads)
	for i := 0; i < int(numtreads); i++ {
		chunks[i].chunkSize = bufferSize
		chunks[i].offset = int64(bufferSize * i)
	}

	if rem := fileSize % int64(bufferSize); rem != 0 {
		c := chunk{chunkSize: int(rem), offset: int64(numtreads * int64(bufferSize))}

		numtreads++
		chunks = append(chunks, c)
	}

	var strChansize int = int(numtreads)
	if strChansize == 0 {
		strChansize++
	}
	strChan := make(chan string, strChansize)
	var wg sync.WaitGroup
	wg.Add(int(numtreads))
	for i := 0; i < int(numtreads); i++ {
		go func(chunks []chunk, i int) {
			defer wg.Done()
			chk := chunks[i]
			buffer := make([]byte, chk.chunkSize)
			_, err := file.ReadAt(buffer, chk.offset)
			if err != nil {
				fmt.Println(err)
				return
			}

			strChan <- string(buffer)

		}(chunks, i)
	}

	mapChan := make(chan psedomap, numtreads)
	var nwg sync.WaitGroup
	for i := 0; i < int(numtreads); i++ {
		nwg.Add(1)
		go worker(strChan, mapChan, &nwg)
	}
	tmpData := map[string]stats{}
	go func() {
		for v := range mapChan {
			_, exists := tmpData[v.key]
			if exists {
				tmpData[v.key] = stats{
					min:   min(v.val.min, tmpData[v.key].min),
					max:   max(tmpData[v.key].max, v.val.max),
					sum:   tmpData[v.key].sum + v.val.sum,
					count: tmpData[v.key].count + v.val.count,
				}
			} else {
				tmpData[v.key] = v.val
			}
		}
	}()
	wg.Wait()
	close(strChan)
	nwg.Wait()
	close(mapChan)

	fmt.Println(tmpData)
}
