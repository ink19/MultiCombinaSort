package lazy_sort

import (
	"bufio"
	"os"
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
		sortStruct := NewLazySort("../data/0.data")
		readData := readDataFromFile("../data/0.data")
		convey.Convey("TestNewStruct", func ()  {
			convey.So(sortStruct.FileName, convey.ShouldEqual, "../data/0.data")
		})

		convey.Convey("TestSort", func () {
			lazySortData := make([] uint64, 0)
			for i := range sortStruct.OutChan {
				lazySortData = append(lazySortData, i)
			}
			sort.Slice(readData, func(i, j int) bool { return readData[i] < readData[j] })
			convey.So(readData, convey.ShouldResemble, lazySortData)
		})
	})
}