package main

import (
	"container/heap"
	"context"
	"encoding/base64"
	"fmt"
	"math/big"
	"os"
	"reflect"

	"github.com/go-redis/redis/v8"
	"github.com/jessevdk/go-flags"
)

var opt struct {
	Addr        string `short:"a" long:"addr" description:"Redis Server Address" default:"localhost"`
	Port        int    `short:"p" long:"port" description:"Redis Server Port" default:"6379"`
	Password    string `long:"password" description:"Redis Server Password" default:""`
	DataFile    string `short:"f" long:"data_file" description:"Out File Name"`
	InputNumber int    `short:"n" long:"input_number" description:"Input Number" default:"10"`
}

func readDataFromRedis(rdb *redis.Client, dataOutput chan *big.Int, batchIndex int) {
	redisDataList := fmt.Sprintf("List_%d", batchIndex)
	redisFlagList := fmt.Sprintf("RList_%d", batchIndex)
	for {
		dataStr, err := rdb.BRPopLPush(context.TODO(), redisDataList, redisFlagList, -1).Result()
		if err != nil {
			panic(err)
		}

		if dataStr == "-1" { // 如果输入为-1,说明该通道数据已经完成
			fmt.Printf("Chan %s Over\n", redisDataList)
			dataOutput <- big.NewInt(-1)
			break
		}

		base64.StdEncoding.DecodeString(dataStr)
		dataBytes, _ := base64.StdEncoding.DecodeString(dataStr)
		dataInt := new(big.Int)
		dataInt.SetBytes(dataBytes)
		dataOutput <- dataInt
		// fmt.Println(dataInt)
	}
	close(dataOutput)
}

func writeFile(dataInput chan *big.Int, filename string) {
	fp, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0666)

	if err != nil {
		panic(err)
	}
	defer fp.Close()

	for iData := range dataInput {
		fp.WriteString(iData.String() + "\n")
	}
}

func mergeData(dataInputs []chan *big.Int, dataOutput chan *big.Int) {
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
		if newQueueItem.priority.Cmp(big.NewInt(-1)) == 0 {
			continue
		}
		heap.Push(&dataQueue, newQueueItem)
	}
	close(dataOutput)
}

func startCommit(rdb *redis.Client, inputNumber int,initFunc func(int)) {
	sCommitChanName := make([] string, inputNumber) // fmt.Sprintf("sCommit_%d", inputNumber)
	cCommitChanName := make([] string, inputNumber) // fmt.Sprintf("cCommit_%d", inputNumber)
	sCommit := make([] *redis.PubSub, inputNumber)
	sCommitChan := make([] <-chan *redis.Message, inputNumber)
	sCommitChanSet := make([] reflect.SelectCase, inputNumber)
	sCommitStatus := make([] int, inputNumber)

	for i := 0; i < inputNumber; i++ {
		sCommitChanName[i] = fmt.Sprintf("sCommit_%d", i)
		cCommitChanName[i] = fmt.Sprintf("cCommit_%d", i)
		sCommit[i] = rdb.Subscribe(context.TODO(), sCommitChanName[i])
		defer sCommit[i].Close()

		sCommitChan[i] = sCommit[i].Channel()
		sCommitChanSet[i] = reflect.SelectCase{
			Dir: reflect.SelectRecv,
			Chan: reflect.ValueOf(sCommitChan[i]),
		}
		sCommitStatus[i] = 0
		
		rdb.Publish(context.TODO(), cCommitChanName[i], "0")
	}

	sumStatus := 0

	for {
		from_index, valInterface, _ := reflect.Select(sCommitChanSet)
		value := valInterface.Interface().(*redis.Message)
		
		// 收到Hello 直接返回 Hello
		if value.Payload == "0" {
			fmt.Printf("Get Hello From %d\n", from_index)
			rdb.Publish(context.TODO(), cCommitChanName[from_index], "0")
		}

		// 收到准备好的消息，返回准备好，并修改状态
		if value.Payload == "1" {
			fmt.Printf("Get Inited From %d\n", from_index)
			sumStatus += (1 - sCommitStatus[from_index])
			sCommitStatus[from_index] = 1
			rdb.Publish(context.TODO(), cCommitChanName[from_index], "1")
		}

		// 收到开始发送标志
		if value.Payload == "2" {
			if sCommitStatus[from_index] == 1 {
				fmt.Printf("Get Ready Begin From %d, And corrent status\n", from_index)
				// 状态正确，结束沟通
				sumStatus += (2 - sCommitStatus[from_index])
				sCommitStatus[from_index] = 2
				initFunc(from_index)
			} else {
				fmt.Printf("Get Ready Begin From %d, But incorrent status\n", from_index)
				// 状态出错，重新沟通
				sumStatus += (0 - sCommitStatus[from_index])
				sCommitStatus[from_index] = 0
				rdb.Publish(context.TODO(), cCommitChanName[from_index], "0")
			}
		}

		if sumStatus == 2 * inputNumber {
			break
		}
	}
	fmt.Println("Commit Over.")
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

	startCommit(rdb, opt.InputNumber, func(i int) {})

	redisDataChannels := make([]chan *big.Int, opt.InputNumber)
	for i := 0; i < opt.InputNumber; i++ {
		redisDataChannels[i] = make(chan *big.Int)
		// append(redisDataChannels, make(chan int))
		go readDataFromRedis(rdb, redisDataChannels[i], i)
	}
	fileData := make(chan *big.Int)

	go mergeData(redisDataChannels, fileData)
	writeFile(fileData, opt.DataFile)
}
