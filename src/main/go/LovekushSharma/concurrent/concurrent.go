package concurrent

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
)

type chunk struct {
	bufsize int
	offset  int64
}

type stats struct {
	min, max, sum float64
	count         int64
}

func Concurrent(filePath string) {
	const BufferSize = 1024 * 1024
	file, err := os.Open(filePath)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer file.Close()

	fileinfo, err := file.Stat()
	if err != nil {
		fmt.Println(err)
		return
	}

	filesize := int(fileinfo.Size())
	concurrency := filesize / BufferSize
	chunksizes := make([]chunk, concurrency)
	for i := 0; i < concurrency; i++ {
		chunksizes[i].bufsize = BufferSize
		chunksizes[i].offset = int64(BufferSize * i)
	}
	if remainder := filesize % BufferSize; remainder != 0 {
		c := chunk{bufsize: remainder, offset: int64(concurrency * BufferSize)}
		concurrency++
		chunksizes = append(chunksizes, c)
	}

	ch := make(chan string, concurrency/10)
	var wg sync.WaitGroup
	wg.Add(concurrency)

	for i := 0; i < concurrency; i++ {
		go func(chunksizes []chunk, i int) {
			defer wg.Done()

			chunk := chunksizes[i]
			buffer := make([]byte, chunk.bufsize)
			_, err := file.ReadAt(buffer, chunk.offset)

			if err != nil {
				fmt.Println(err)
				return
			}
			ch <- string(buffer)
		}(chunksizes, i)
	}
	tmpData := make(map[string]stats)
	d := make(chan bool)
	go func() {
		for v := range ch {
			ls := strings.Split(v, "\n")
			for _, s := range ls {
				ss := strings.Split(s, ";")
				if len(ss) < 2 {
					continue
				}
				ss[1], _ = strings.CutSuffix(ss[1], "\r")
				v, err := strconv.ParseFloat(ss[1], 64)
				if err != nil {
					continue
				}

				_, exists := tmpData[ss[0]]
				if exists {
					tmpData[ss[0]] = stats{min: v, max: v, sum: v, count: 1}
				} else {
					tmpData[ss[0]] = stats{
						min:   min(tmpData[ss[0]].min, v),
						max:   max(tmpData[ss[0]].max, v),
						sum:   tmpData[ss[0]].sum + v,
						count: tmpData[ss[0]].count + 1,
					}
				}

			}
		}
		d <- true
	}()
	wg.Wait()
	close(ch)
	<-d
	for k, v := range tmpData {
		fmt.Printf("<%s;%.2f,%.2f,%.2f>", k, v.min, v.max, v.sum/float64(v.count))
	}
}
