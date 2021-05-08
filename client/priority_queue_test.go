package main

import (
	"container/heap"
	"fmt"
	"math/big"
	"sort"
	"testing"

	"github.com/smartystreets/goconvey/convey"
)

func TestQueue(t *testing.T) {
	queue := make(PriorityQueue, 0)
	heap.Init(&queue)
	// var q *PriorityQueue
	convey.Convey("TestQueue", t, func () {
		convey.Convey("PushAndLen", func () {
			convey.So(queue.Len(), convey.ShouldEqual, 0)
			queue.Push(&Item{
				channel_index: 1024,
				priority: big.NewInt(10),
			})
			convey.So(queue.Len(), convey.ShouldEqual, 1)
			queue.Pop()
			convey.So(queue.Len(), convey.ShouldEqual, 0)
		})

		convey.Convey("PushAndPop", func () {
			convey.So(queue.Len(), convey.ShouldEqual, 0)
			input_data := []int{1, 2, 3, 4, 12, 5, 6, 9, 24, 12, 2, 4}
			for _, idata := range input_data {
				heap.Push(&queue, &Item{
					channel_index: idata,
					priority: big.NewInt(int64(idata)),
				})
			}
			convey.So(queue.Len(), convey.ShouldEqual, len(input_data))
			out_data := make([] int, 0)
			for queue.Len() != 0 {
				odata := heap.Pop(&queue).(*Item)
				out_data = append(out_data, int(odata.priority.Int64()))
			}
			convey.So(sort.SliceIsSorted(out_data, func(i, j int) bool {return out_data[i] < out_data[j]}), convey.ShouldEqual, true)
		})

		convey.Convey("Update", func () {
			convey.So(queue.Len(), convey.ShouldEqual, 0)
			input_data := []int{1, 2, 3, 4, 12, 5, 6, 9, 24, 12, 2, 4}
			for _, idata := range input_data {
				heap.Push(&queue, &Item{
					channel_index: idata,
					priority: big.NewInt(int64(idata)),
				})
			}
			convey.So(queue.Len(), convey.ShouldEqual, len(input_data))

			queue.Update(queue[4], 60, big.NewInt(60))

			out_data := make([]int, 0)
			for queue.Len() != 0 {
				odata := heap.Pop(&queue).(*Item)
				out_data = append(out_data, int(odata.priority.Int64()))
			}
			fmt.Println(out_data)
			convey.So(sort.SliceIsSorted(out_data, func(i, j int) bool {return out_data[i] < out_data[j]}), convey.ShouldEqual, true)
		})
	})
}