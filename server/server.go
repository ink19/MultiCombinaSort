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
	DataPrefix  string `long:"data_prefix" description:"Read File Prefix"`
	DataIndex string `long:"data_index" description:"Read File Index"`
	QueueSize int    `short:"q" long:"queue_size" description:"redis queue size" default:"10"`
}

var server_config struct {
	RedisDataList string
	RedisFlagList string
	DataFilePath string
	RedisAddr string
	RedisPass string
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
		rdb.BRPop(-1, server_config.RedisFlagList)
		rdb.LPush(server_config.RedisDataList, data_item)
	}
	// Over Flag
	rdb.LPush(server_config.RedisDataList, -1)
}

func init_redis_list(rdb *redis.Client) {
	rdb.Del(server_config.RedisDataList)
	rdb.Del(server_config.RedisFlagList)
	for i := 0; i < opt.QueueSize; i++ {
		rdb.LPush(server_config.RedisFlagList, -1)
	}
}

func main() {
	_, err := flags.Parse(&opt)
	if err != nil {
		panic(err)
	}
	fmt.Println(opt)
	server_config.RedisDataList = "List_" + opt.DataIndex
	server_config.RedisFlagList = "RList_" + opt.DataIndex
	server_config.DataFilePath = opt.DataPrefix + opt.DataIndex + ".data"
	server_config.RedisAddr = fmt.Sprintf("%s:%d", opt.Addr, opt.Port)
	server_config.RedisPass = opt.Password

	source_data := read_data(server_config.DataFilePath)
	// fmt.Println(source_data)
	sort.Slice(source_data, func(i, j int) bool { return source_data[i] < source_data[j] })
	// fmt.Println(source_data)
	rdb := redis.NewClient(&redis.Options{
		Addr:     server_config.RedisAddr,
		Password: server_config.RedisPass,
	})
	defer rdb.Close()
	init_redis_list(rdb)
	send_data(source_data, rdb)
}
