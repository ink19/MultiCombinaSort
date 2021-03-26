package main

import (
	"bufio"
	"fmt"
	"os"
	"sort"
	"strconv"

	"github.com/go-redis/redis"
	"github.com/jessevdk/go-flags"
)

var opt struct {
	Addr      string `short:"a" long:"addr" description:"Redis Server Address" default:"localhost"`
	Port      int    `short:"p" long:"port" description:"Redis Server Port" default:"6379"`
	Password  string `long:"password" description:"Redis Server Password" default:""`
	DataFile  string `short:"f" long:"data_file" description:"Read File Name"`
	QueueSize int    `short:"q" long:"queue_size" description:"redis queue size" default:"10"`
}

func read_data(filename string) []int {
	return_data := make([]int, 0)
	// 打开文件
	fp, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer fp.Close()

	fp_scan := bufio.NewScanner(fp)

	for fp_scan.Scan() {
		return_item, _ := strconv.ParseInt(fp_scan.Text(), 10, 64)
		return_data = append(return_data, int(return_item))
	}

	return return_data
}

func send_data(data []int, rdb *redis.Client) {

	for _, data_item := range data {
		rdb.BRPop(-1, "RList_1")
		rdb.LPush("List_1", data_item)
	}
	// Over Flag
	rdb.LPush("List_1", -1)
}

func init_redis_list(rdb *redis.Client) {
	rdb.Del("List_1")
	rdb.Del("RList_1")
	for i := 0; i < opt.QueueSize; i++ {
		rdb.LPush("RList_1", -1)
	}
}

func main() {
	// ctx := context.Background()
	_, err := flags.Parse(&opt)
	if err != nil {
		panic(err)
	}
	fmt.Println(opt)
	source_data := read_data(opt.DataFile)
	// fmt.Println(source_data)
	sort.Slice(source_data, func(i, j int) bool { return source_data[i] < source_data[j] })
	// fmt.Println(source_data)
	rdb := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", opt.Addr, opt.Port),
		Password: opt.Password,
	})
	defer rdb.Close()
	init_redis_list(rdb)
	send_data(source_data, rdb)
}
