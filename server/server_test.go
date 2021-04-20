package main

import (
	"math/rand"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey"
	"github.com/go-redis/redis"
	"github.com/smartystreets/goconvey/convey"
)

func TestSendDataToRedis(t *testing.T) {
	convey.Convey("TestSendDataToRedis", t, func() {
		testData := make([]int, 0)
		for i := 0; i < 100000; i++ {
			testData = append(testData, rand.Int())
		}
		rdb := redis.NewClient(&redis.Options{})
		brpop_times := 0
		lpush_times := 0
		pushData := make([] int, 0)

		brpopPile := gomonkey.ApplyFunc(rdb.BRPop , func(timeout time.Duration, keys ...string) *redis.StringSliceCmd {
			brpop_times ++
			return nil
		})
		defer brpopPile.Reset()

		lpushPile := gomonkey.ApplyFunc(rdb.LPush, func(key string, values ...interface{}) *redis.IntCmd {
			lpush_times ++
			pushData = append(pushData, values[0].(int))
			return nil
		})
		defer lpushPile.Reset()

		dataChan := make(chan int, 1)

		go func() {
			for _, idata := range testData {
				dataChan <- idata
			}
			close(dataChan)
		}()
		
		sendDataToRedis(dataChan, rdb, "", "")
		convey.So(brpop_times, convey.ShouldEqual, len(testData))
		convey.So(lpush_times, convey.ShouldEqual, len(testData) + 1)
		convey.So(pushData, convey.ShouldResemble, testData)
	})
}


