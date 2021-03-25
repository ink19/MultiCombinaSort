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
}

func read_data_from_redis(rdb *redis.Client, data_channel chan int) {
	for {
		sdata, _ := rdb.BRPop(0, "List_1").Result()
		rdb.Publish("Signal_1", "go")
		idata, _ := strconv.ParseInt(sdata[1], 10, 64)
		if idata == -1 {
			break
		}
		data_channel <- int(idata)
	}
	close(data_channel)
}

func write_data_to_file(data_channel chan int, filename string) {
	fp, err := os.OpenFile(filename, os.O_CREATE | os.O_WRONLY, 0666)
	if err != nil {
		panic(err)
	}

	for idata := range data_channel {
		fp.WriteString(fmt.Sprintln(idata))
	}
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

	data_chan := make(chan int)
	go read_data_from_redis(rdb, data_chan)
	write_data_to_file(data_chan, opt.DataFile)
}