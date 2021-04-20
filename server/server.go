package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/go-redis/redis"
	"github.com/ink19/MultiCombinaSort/server/lazy_sort"
	"github.com/jessevdk/go-flags"
)

var opt struct {
	Addr       string `short:"a" long:"addr" description:"Redis Server Address" default:"localhost"`
	Port       int    `short:"p" long:"port" description:"Redis Server Port" default:"6379"`
	Password   string `long:"password" description:"Redis Server Password" default:""`
	DataPrefix string `long:"data_prefix" description:"Read File Prefix"`
	DataIndex  string `long:"data_index" description:"Read File Index"`
	QueueSize  int    `short:"q" long:"queue_size" description:"redis queue size" default:"10"`
}

var servConfig struct {
	RedisDataList  string
	RedisFlagList  string
	DataFilePath   string
	RedisAddr      string
	RedisPass      string
	redisWatchFlag string
}


func sendDataToRedis(fileData chan int, rdb *redis.Client, redisDataList string, redisFlagList string) {

	for fileDataItem := range fileData {
		rdb.BRPop(-1, redisFlagList)
		rdb.LPush(redisDataList, fileDataItem)
	}
	// Over Flag
	rdb.LPush(redisDataList, -1)
}

func initRedisList(rdb *redis.Client, redisDataList string, redisFlagList string, redisWatchFlag string) {
	err := rdb.Watch(
		func(t *redis.Tx) error {
			_, err := t.Get(redisWatchFlag).Int()
			if err != nil && err != redis.Nil {
				return err
			}

			if err != redis.Nil {
				return redis.TxFailedErr
			}

			t.Set(redisWatchFlag, "1", 0)
			return nil
		},
		redisWatchFlag)

	if err != nil && err != redis.TxFailedErr {
		panic(err)
	}

	if err == redis.TxFailedErr {
		panic("Other Get It.")
	}

	rdb.Del(redisDataList)
	rdb.Del(redisFlagList)
	for i := 0; i < opt.QueueSize; i++ {
		rdb.LPush(redisFlagList, -1)
	}
}

func clearEnv(rdb *redis.Client, redisDataList string, redisFlagList string, redisWatchFlag string) {
	fmt.Println("Clear Environment")
	rdb.Del(redisWatchFlag)
	rdb.Del(redisDataList)
	rdb.Del(redisFlagList)
	rdb.Close()
}

func signalCatch(signalChan chan os.Signal, rdb *redis.Client, redisDataList string, redisFlagList string, redisWatchFlag string) {
	<- signalChan
	fmt.Println("Get Ctrl+C")
	clearEnv(rdb, redisDataList, redisFlagList, redisWatchFlag)
	os.Exit(0)
}

func main() {
	_, err := flags.Parse(&opt)
	if err != nil {
		panic(err)
	}
	fmt.Println(opt)
	servConfig.RedisDataList = "List_" + opt.DataIndex
	servConfig.RedisFlagList = "RList_" + opt.DataIndex
	servConfig.redisWatchFlag = "Watch_"+opt.DataIndex
	servConfig.DataFilePath = opt.DataPrefix + opt.DataIndex + ".data"
	servConfig.RedisAddr = fmt.Sprintf("%s:%d", opt.Addr, opt.Port)
	servConfig.RedisPass = opt.Password

	lazySort := lazy_sort.NewLazySort(servConfig.DataFilePath)

	rdb := redis.NewClient(&redis.Options{
		Addr:     servConfig.RedisAddr,
		Password: servConfig.RedisPass,
	})
	defer clearEnv(rdb, servConfig.RedisDataList, servConfig.RedisFlagList, servConfig.redisWatchFlag)

	err = rdb.Ping().Err()

	if (err != nil) {
		panic(err)
	}

	initRedisList(rdb, servConfig.RedisDataList, servConfig.RedisFlagList, servConfig.redisWatchFlag)

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT)
	go signalCatch(signalChan, rdb, servConfig.RedisDataList, servConfig.RedisFlagList, servConfig.redisWatchFlag)

	sendDataToRedis(lazySort.OutChan, rdb, servConfig.RedisDataList, servConfig.RedisFlagList)
}
