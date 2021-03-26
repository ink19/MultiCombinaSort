package main

import (
	"fmt"
	"os"
	"strconv"

	"github.com/go-redis/redis"
	"github.com/jessevdk/go-flags"
)

var opt struct {
	Addr     string `short:"a" long:"addr" description:"Redis Server Address" default:"localhost"`
	Port     int    `short:"p" long:"port" description:"Redis Server Port" default:"6379"`
	Password string `long:"password" description:"Redis Server Password" default:""`
	DataFile string `short:"f" long:"data_file" description:"Out File Name"`
	InputNumber int `short:"n" long:"input_number" description:"Input Number" default:"10"`
}

func read_data_from_redis(rdb *redis.Client, data_channel chan int, data_index int) {
	redis_data_list := fmt.Sprintf("List_%d", data_index)
	redis_flag_list := fmt.Sprintf("RList_%d", data_index)
	for {
		sdata, err := rdb.BRPopLPush(redis_data_list, redis_flag_list, -1).Result()
		
		if err != nil {
			panic(err)
		}

		idata, _ := strconv.ParseInt(sdata, 10, 64)
		data_channel <- int(idata)
		if idata == -1 { // 如果输入为-1,说明该通道数据已经完成
			break
		}
	}
	close(data_channel)
}

func write_data_to_file(data_channel chan int, filename string) {
	fp, err := os.OpenFile(filename, os.O_CREATE | os.O_WRONLY, 0666)

	if err != nil {
		panic(err)
	}
	defer fp.Close()
	
	for idata := range data_channel {
		fp.WriteString(fmt.Sprintln(idata))
	}
}

func channel_multi_to_one(multi_data_channel []chan int, out_channel chan int) {
	mqueue := make(PriorityQueue, 0)
	for channel_index, mchannel := range multi_data_channel {
		item := &Item{
			channel_index: channel_index,
			priority: <-mchannel,
		}
		mqueue.Push(item)
	}
	for mqueue.Len() != 0 {
		oitem := mqueue.Pop().(*Item)
		out_channel <- oitem.priority
		iitem := &Item {
			channel_index: oitem.channel_index,
			priority: <- multi_data_channel[oitem.channel_index],
		}
		if iitem.priority == -1 {
			continue
		}
		mqueue.Push(iitem)
	}
	close(out_channel)
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

	redis_data_chan := make([] chan int, opt.InputNumber) 
	for i := 0; i < opt.InputNumber; i++ {
		redis_data_chan[i] = make(chan int)
		// append(redis_data_chan, make(chan int))
		go read_data_from_redis(rdb, redis_data_chan[i], i)
	}
	write_data_chan := make(chan int)
	
	go channel_multi_to_one(redis_data_chan, write_data_chan)
	write_data_to_file(write_data_chan, opt.DataFile)
}