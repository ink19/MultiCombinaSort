package lazy_sort

import (
	"bufio"
	"os"
	"strconv"
)

type LazySort struct {
	FileName string
	data     []int
	OutChan  chan int
}

func readDataFromFile(fileName string) []int {
	fileData := make([]int, 0)
	// 打开文件
	fp, err := os.Open(fileName)
	if err != nil {
		panic(err)
	}
	defer fp.Close()

	fileScanner := bufio.NewScanner(fp)
	for fileScanner.Scan() {
		readItem, err := strconv.ParseInt(fileScanner.Text(), 10, 64)

		if err != nil {
			panic(err)
		}

		fileData = append(fileData, int(readItem))
	}

	return fileData
}

func NewLazySort(filename string) *LazySort {
	p := new(LazySort)
	p.FileName = filename
	p.OutChan = make(chan int)
	p.data = readDataFromFile(filename)
	go p.sortIt(0, len(p.data))
	return p
}

func (p * LazySort) sortIt(begin, end int) {
	if (begin >= end) {
		return
	}

	if (begin + 1 == end) {
		p.OutChan <- p.data[begin]
		return
	}
	
	left := begin + 1
	right := end - 1

	for left <= right {
		if p.data[left] > p.data[begin] {
			// temp := p.data[left]
			// p.data[left] = p.data[right]
			// p.data[right] = temp
			p.data[left], p.data[right] = p.data[right], p.data[left]
			right --
		} else {
			left ++
		}
	}

	p.data[begin], p.data[right] = p.data[right], p.data[begin]

	p.sortIt(begin, right)
	p.OutChan <- p.data[right]
	p.sortIt(right + 1, end)

	if (begin == 0 && end == len(p.data)) {
		close(p.OutChan)
	}
}