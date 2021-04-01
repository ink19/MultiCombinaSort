package main

import (
	"container/heap"
	"fmt"
	"os"
	"strconv"

	"github.com/go-redis/redis"
	"github.com/jessevdk/go-flags"
)

var opt struct {
	Addr        string `short:"a" long:"addr" description:"Redis Server Address" default:"localhost"`
	Port        int    `short:"p" long:"port" description:"Redis Server Port" default:"6379"`
	Password    string `long:"password" description:"Redis Server Password" default:""`
	DataFile    string `short:"f" long:"data_file" description:"Out File Name"`
	InputNumber int    `short:"n" long:"input_number" description:"Input Number" default:"10"`
}

func readDataFromRedis(rdb *redis.Client, dataOutput chan int, batchIndex int) {
	redisDataList := fmt.Sprintf("List_%d", batchIndex)
	redisFlagList := fmt.Sprintf("RList_%d", batchIndex)
	for {
		dataStr, err := rdb.BRPopLPush(redisDataList, redisFlagList, -1).Result()
		if err != nil {
			panic(err)
		}

		dataInt, _ := strconv.ParseInt(dataStr, 10, 64)
		dataOutput <- int(dataInt)
		if dataInt == -1 { // 如果输入为-1,说明该通道数据已经完成
			break
		}
	}
	close(dataOutput)
}

func writeFile(dataInput chan int, filename string) {
	fp, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0666)

	if err != nil {
		panic(err)
	}
	defer fp.Close()

	for iData := range dataInput {
		fp.WriteString(fmt.Sprintln(iData))
	}
}

func mergeData(dataInputs []chan int, dataOutput chan int) {
	dataQueue := make(PriorityQueue, 0)
	for dataInputIndex, dataInputItem := range dataInputs {
		item := &Item{
			channel_index: dataInputIndex,
			priority:      <-dataInputItem,
		}
		heap.Push(&dataQueue, item)
	}
	for dataQueue.Len() != 0 {
		minQueueItem := heap.Pop(&dataQueue).(*Item)
		dataOutput <- minQueueItem.priority

		newQueueItem := &Item{
			channel_index: minQueueItem.channel_index,
			priority:      <-dataInputs[minQueueItem.channel_index],
		}
		if newQueueItem.priority == -1 {
			continue
		}
		heap.Push(&dataQueue, newQueueItem)
	}
	close(dataOutput)
}

func main() {
	_, err := flags.Parse(&opt)
	if err != nil {
		panic(err)
	}
	fmt.Println(opt)

	rdb := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", opt.Addr, opt.Port),
		Password: opt.Password,
	})
	defer rdb.Close()

	redisDataChannels := make([]chan int, opt.InputNumber)
	for i := 0; i < opt.InputNumber; i++ {
		redisDataChannels[i] = make(chan int)
		// append(redisDataChannels, make(chan int))
		go readDataFromRedis(rdb, redisDataChannels[i], i)
	}
	fileData := make(chan int)

	go mergeData(redisDataChannels, fileData)
	writeFile(fileData, opt.DataFile)
}
