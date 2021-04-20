package lazy_sort

import (
	"sort"
	"testing"

	"github.com/smartystreets/goconvey/convey"
)

func TestNewLazySort(t *testing.T) {
	convey.Convey("TestNewLazySort", t, func () {
		sortStruct := NewLazySort("../data/0.data")
		readData := readDataFromFile("../data/0.data")
		convey.Convey("TestNewStruct", func ()  {
			convey.So(sortStruct.FileName, convey.ShouldEqual, "../data/0.data")
		})

		convey.Convey("TestSort", func () {
			lazySortData := make([] int, 0)
			for i := range sortStruct.OutChan {
				lazySortData = append(lazySortData, i)
			}
			sort.Slice(readData, func(i, j int) bool { return readData[i] < readData[j] })
			convey.So(readData, convey.ShouldResemble, lazySortData)
		})
	})
}