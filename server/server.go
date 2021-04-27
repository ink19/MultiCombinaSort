package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/go-redis/redis/v8"
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

func sendDataToRedis(fileData chan uint64, rdb *redis.Client, redisDataList string, redisFlagList string) {

	for fileDataItem := range fileData {
		_, err := rdb.BRPop(context.TODO(), -1, redisFlagList).Result()

		if err != nil {
			panic(err)
		}

		_, err = rdb.LPush(context.TODO(), redisDataList, fileDataItem).Result()
		
		if err != nil {
			panic(err)
		}
	}
	// Over Flag
	_, err := rdb.LPush(context.TODO(), redisDataList, -1).Result()

	if err != nil {
		panic(err)
	}
}

func initRedisList(rdb *redis.Client, redisDataList string, redisFlagList string, redisWatchFlag string) {
	err := rdb.Watch(
		context.TODO(),
		func(t *redis.Tx) error {
			_, err := t.Get(context.TODO(), redisWatchFlag).Int()
			if err != nil && err != redis.Nil {
				return err
			}

			if err != redis.Nil {
				return redis.TxFailedErr
			}

			_, err = t.Set(context.TODO(), redisWatchFlag, "1", 0).Result()

			return err
		},
		redisWatchFlag)

	if err != nil && err != redis.TxFailedErr {
		panic(err)
	}

	if err == redis.TxFailedErr {
		panic("Other Get It.")
	}

	_, err = rdb.Del(context.TODO(), redisDataList).Result()

	if err != nil {
		panic(err)
	}

	_, err = rdb.Del(context.TODO(), redisFlagList).Result()
	
	if err != nil {
		panic(err)
	}

	for i := 0; i < opt.QueueSize; i++ {
		_, err = rdb.LPush(context.TODO(), redisFlagList, -1).Result()
		if err != nil {
			panic(err)
		}
	}
}

func clearEnv(rdb redis.Cmdable, redisDataList string, redisFlagList string, redisWatchFlag string) {
	fmt.Println("Clear Environment")
	rdb.Del(context.TODO(), redisWatchFlag)
	rdb.Del(context.TODO(), redisDataList)
	rdb.Del(context.TODO(), redisFlagList)
}

func signalCatch(signalChan chan os.Signal) {
	<- signalChan
	panic("Get Ctrl+C")
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
	defer rdb.Close()
	defer clearEnv(rdb, servConfig.RedisDataList, servConfig.RedisFlagList, servConfig.redisWatchFlag)

	err = rdb.Ping(context.TODO()).Err()

	if (err != nil) {
		panic(err)
	}

	initRedisList(rdb, servConfig.RedisDataList, servConfig.RedisFlagList, servConfig.redisWatchFlag)

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT)
	go signalCatch(signalChan)

	sendDataToRedis(lazySort.OutChan, rdb, servConfig.RedisDataList, servConfig.RedisFlagList)
}
