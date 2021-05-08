package lazysort

import (
	"bufio"
	"fmt"
	"math/big"
	"os"
	"runtime"
	"sort"
	"strconv"
	"testing"

	"github.com/smartystreets/goconvey/convey"
)

func readDataFromFile(fileName string) [] uint64 {
	fileData := make([] uint64, 0)
	// 打开文件
	fp, err := os.Open(fileName)
	if err != nil {
		panic(err)
	}
	defer fp.Close()

	fileScanner := bufio.NewScanner(fp)
	for fileScanner.Scan() {
		readItem, err := strconv.ParseUint(fileScanner.Text(), 10, 64)

		if err != nil {
			panic(err)
		}

		fileData = append(fileData, readItem)
	}

	return fileData
}

func TestNewLazySort(t *testing.T) {
	convey.Convey("TestNewLazySort", t, func () {
		sortStruct := NewLazySort("../data/0.data", 8)
		readData := readDataFromFile("../data/0.data")
		convey.Convey("TestNewStruct", func ()  {
			convey.So(sortStruct.FileName, convey.ShouldEqual, "../data/0.data")
		})

		convey.Convey("TestSort", func () {
			lazySortData := make([] * big.Int, 0)
			for i := range sortStruct.OutChan {
				lazySortData = append(lazySortData, i)
			}
			sort.Slice(readData, func(i, j int) bool { return readData[i] < readData[j] })
			for i := 0; i < len(readData); i++ {
				convey.So(lazySortData[i].Cmp(big.NewInt(int64(readData[i]))), convey.ShouldEqual, 0)
			}
		})
	})
}

func bToKb(b uint64) uint64 {
	return b / 1024
}

func PrintMemUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	// For info on each, see: https://golang.org/pkg/runtime/#MemStats
	fmt.Printf("Alloc = %v KiB", bToKb(m.Alloc))
	fmt.Printf("\tTotalAlloc = %v KiB", bToKb(m.TotalAlloc))
	fmt.Printf("\tSys = %v KiB", bToKb(m.Sys))
	fmt.Printf("\tNumGC = %v\n", m.NumGC)
}

func BenchmarkLazySort(b *testing.B) {
	sortStruct := NewLazySort("../data/0.data", 8)
	num := 0
	for range sortStruct.OutChan {
		// Use GC To Gc
		runtime.GC()
		num++
		if num % 1000 == 0 {
			PrintMemUsage()
		}
	}
}