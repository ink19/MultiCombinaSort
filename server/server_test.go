package main

import (
	"errors"
	"math/rand"
	"testing"

	"github.com/go-redis/redismock/v8"
	"github.com/smartystreets/goconvey/convey"
)

func TestSendDataToRedis(t *testing.T) {
	convey.Convey("TestSendDataToRedisSuccessful", t, func() {
		testData := make([]int, 0)
		for i := 0; i < 1000; i++ {
			testData = append(testData, rand.Int())
		}

		db, mock := redismock.NewClientMock()
		dataChan := make(chan int, 1)
		// panic(err)

		go func() {
			for _, idata := range testData {
				mock.ExpectBRPop(-1, "BRPop").SetVal([] string {"1"})
				mock.Regexp().ExpectLPush("LPush", idata).SetVal(1)
				dataChan <- idata
			}
			mock.Regexp().ExpectLPush("LPush", -1).SetVal(1)
			close(dataChan)
		}()
		
		convey.So(func() {
			sendDataToRedis(dataChan, db, "LPush", "BRPop")
		}, convey.ShouldNotPanic)
		convey.So(mock.ExpectationsWereMet(), convey.ShouldEqual, nil)
		// convey.So(brpop_times, convey.ShouldEqual, len(testData))
		// convey.So(lpush_times, convey.ShouldEqual, len(testData) + 1)
		// convey.So(pushData, convey.ShouldResemble, testData)
	})

	convey.Convey("TestSendDataToRedisPanic", t, func() {
		testData := make([]int, 0)
		for i := 0; i < 1000; i++ {
			testData = append(testData, rand.Int())
		}

		db, mock := redismock.NewClientMock()
		dataChan := make(chan int, 1)

		go func() {
			for _, idata := range testData {
				mock.ExpectBRPop(-1, "BRPop").SetVal([] string {"1"})
				mock.Regexp().ExpectLPush("LPush", idata).SetVal(1)
				dataChan <- idata
			}
			mock.Regexp().ExpectLPush("LPush", -1).SetErr(errors.New("Panic Test"))
			close(dataChan)
		}()

		convey.So(func() {
			sendDataToRedis(dataChan, db, "LPush", "BRPop")
		}, convey.ShouldPanic)
		
		dataChan = make(chan int, 1)
		go func() {
			mock.ExpectBRPop(-1, "BRPop").SetErr(errors.New("Panic Test"))
			mock.Regexp().ExpectLPush("LPush", 1025).SetVal(1)
			dataChan <- 1025
		}()
		convey.So(func() {
			sendDataToRedis(dataChan, db, "LPush", "BRPop")
		}, convey.ShouldPanic)
		
		dataChan = make(chan int, 1)
		go func() {
			mock.ExpectBRPop(-1, "BRPop").SetVal([] string {"1"})
			mock.Regexp().ExpectLPush("LPush", 1025).SetErr(errors.New("Panic Test"))
			dataChan <- 1025
		}()
		convey.So(func() {
			sendDataToRedis(dataChan, db, "LPush", "BRPop")
		}, convey.ShouldPanic)
	})
}


