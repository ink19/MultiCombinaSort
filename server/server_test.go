package main

import (
	"context"
	"encoding/base64"
	"errors"
	"math/big"
	"math/rand"
	"os"
	"strconv"
	"syscall"
	"testing"

	"github.com/agiledragon/gomonkey"
	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"
	"github.com/go-redis/redismock/v8"
	"github.com/ink19/MultiCombinaSort/server/lazysort"
	"github.com/jessevdk/go-flags"
	"github.com/smartystreets/goconvey/convey"
)

func TestSendDataToRedis(t *testing.T) {
	convey.Convey("TestSendDataToRedisSuccessful", t, func() {
		testData := make([] int64, 0)
		for i := 0; i < 1000; i++ {
			testData = append(testData, rand.Int63())
		}

		db, mock := redismock.NewClientMock()
		dataChan := make(chan *big.Int, 1)
		// panic(err)

		go func() {
			for _, idata := range testData {
				mock.ExpectBRPop(-1, "BRPop").SetVal([] string {"1"})
				mock.ExpectLPush("LPush", base64.StdEncoding.EncodeToString(big.NewInt(int64(idata)).Bytes())).SetVal(1)
				dataChan <- big.NewInt(int64(idata))
			}
			mock.ExpectLPush("LPush", -1).SetVal(1)
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
		testData := make([] int64, 0)
		for i := 0; i < 1000; i++ {
			testData = append(testData, rand.Int63())
		}

		db, mock := redismock.NewClientMock()
		dataChan := make(chan *big.Int, 1)

		go func() {
			for _, idata := range testData {
				mock.ExpectBRPop(-1, "BRPop").SetVal([] string {"1"})
				mock.ExpectLPush("LPush", base64.StdEncoding.EncodeToString(big.NewInt(int64(idata)).Bytes())).SetVal(1)
				dataChan <- big.NewInt(int64(idata))
			}
			mock.ExpectLPush("LPush", -1).SetErr(errors.New("Panic Test"))
			close(dataChan)
		}()

		convey.So(func() {
			sendDataToRedis(dataChan, db, "LPush", "BRPop")
		}, convey.ShouldPanic)
		
		dataChan = make(chan *big.Int, 1)
		go func() {
			mock.ExpectBRPop(-1, "BRPop").SetErr(errors.New("Panic Test"))
			mock.ExpectLPush("LPush", base64.StdEncoding.EncodeToString(big.NewInt(1025).Bytes())).SetVal(1)
			dataChan <- big.NewInt(1025)
		}()
		convey.So(func() {
			sendDataToRedis(dataChan, db, "LPush", "BRPop")
		}, convey.ShouldPanic)
		
		dataChan = make(chan *big.Int, 1)
		go func() {
			mock.ExpectBRPop(-1, "BRPop").SetVal([] string {"1"})
			mock.ExpectLPush("LPush", base64.StdEncoding.EncodeToString(big.NewInt(1025).Bytes())).SetErr(errors.New("Panic Test"))
			dataChan <- big.NewInt(int64(1025))
		}()
		convey.So(func() {
			sendDataToRedis(dataChan, db, "LPush", "BRPop")
		}, convey.ShouldPanic)
	})
}

func TestInitRedisList(t *testing.T) {
	convey.Convey("TestInitRedisListWatch", t, func() {
		convey.Convey("TestInitRedisListWatchChange", func ()  {
			db, mock := redismock.NewClientMock()
			mock.ExpectWatch("Watch").SetErr(redis.TxFailedErr)
			mock.ExpectGet("Watch").RedisNil()
			mock.ExpectSet("Watch", "1", 0)

			convey.So(func() {
				initRedisList(db, "DataList", "FlagList", "Watch")
			}, convey.ShouldPanicWith, "Other Get It.")
		})

		convey.Convey("TestInitRedisListWatchAlready", func ()  {
			db, mock := redismock.NewClientMock()
			mock.ExpectWatch("Watch")
			mock.ExpectGet("Watch").SetVal("1")
			mock.ExpectSet("Watch", "1", 0)

			convey.So(func() {
				initRedisList(db, "DataList", "FlagList", "Watch")
			}, convey.ShouldPanicWith, "Other Get It.")
		})

		convey.Convey("TestInitRedisListWatchSet", func ()  {
			db, mock := redismock.NewClientMock()
			mock.ExpectWatch("Watch")
			mock.ExpectGet("Watch").RedisNil()

			err := errors.New("Panic Test")

			mock.ExpectSet("Watch", "1", 0).SetErr(err)

			convey.So(func() {
				initRedisList(db, "DataList", "FlagList", "Watch")
			}, convey.ShouldPanicWith, err)
		})

		convey.Convey("TestInitRedisListWatchDataListDel", func () {
			db, mock := redismock.NewClientMock()
			mock.ExpectWatch("Watch")
			mock.ExpectGet("Watch").RedisNil()
			mock.ExpectSet("Watch", "1", 0).SetVal("1")

			err := errors.New("Panic Test")

			mock.ExpectDel("DataList").SetErr(err)
			convey.So(func() {
				initRedisList(db, "DataList", "FlagList", "Watch")
			}, convey.ShouldPanicWith, err)
		})

		convey.Convey("TestInitRedisListWatchFlagListDel", func () {
			db, mock := redismock.NewClientMock()
			mock.ExpectWatch("Watch")
			mock.ExpectGet("Watch").RedisNil()
			mock.ExpectSet("Watch", "1", 0).SetVal("1")
			mock.ExpectDel("DataList").SetVal(1)

			err := errors.New("Panic Test")

			mock.ExpectDel("FlagList").SetErr(err)
			convey.So(func() {
				initRedisList(db, "DataList", "FlagList", "Watch")
			}, convey.ShouldPanicWith, err)
		})

		convey.Convey("TestInitRedisListWatchFlagListPush", func ()  {
			db, mock := redismock.NewClientMock()
			mock.ExpectWatch("Watch")
			mock.ExpectGet("Watch").RedisNil()
			mock.ExpectSet("Watch", "1", 0).SetVal("1")
			mock.ExpectDel("DataList").SetVal(1)

			mock.ExpectDel("FlagList").SetVal(1)

			err := errors.New("Panic Test")

			gomonkey.ApplyGlobalVar(&opt.QueueSize, 10)

			mock.ExpectLPush("FlagList", -1).SetErr(err)

			convey.So(func() {
				initRedisList(db, "DataList", "FlagList", "Watch")
			}, convey.ShouldPanicWith, err)
		})

		convey.Convey("TestInitRedisListWatchSuccessful", func ()  {
			db, mock := redismock.NewClientMock()
			mock.ExpectWatch("Watch")
			mock.ExpectGet("Watch").RedisNil()
			mock.ExpectSet("Watch", "1", 0).SetVal("1")
			mock.ExpectDel("DataList").SetVal(1)

			mock.ExpectDel("FlagList").SetVal(1)

			opt_stub := gomonkey.ApplyGlobalVar(&opt.QueueSize, 10)
			defer opt_stub.Reset()

			for i := 0; i < 10; i++ {
				mock.ExpectLPush("FlagList", -1).SetVal(1)
			}

			convey.So(func() {
				initRedisList(db, "DataList", "FlagList", "Watch")
			}, convey.ShouldNotPanic)
			convey.So(mock.ExpectationsWereMet(), convey.ShouldEqual, nil)
		})
	})
}

func TestClearEnv(t *testing.T) {
	convey.Convey("TestClearEnv", t, func ()  {
		db, mock := redismock.NewClientMock()
		mock.ExpectDel("Watch")
		// mock.ExpectDel("DataList")
		// mock.ExpectDel("FlagList")

		clearEnv(db, "DataList", "FlagList", "Watch")

		convey.So(mock.ExpectationsWereMet(), convey.ShouldEqual, nil)
	})
}

func TestSignalCatch(t *testing.T) {
	convey.Convey("TestSignalCatch", t, func ()  {
		signalChan := make(chan os.Signal, 1)
		signalChan <- syscall.SIGINT
		
		convey.So(func() {
			signalCatch(signalChan)
		}, convey.ShouldPanicWith, "Get Ctrl+C")
	})
}

func TestStartCommit(t *testing.T) {
	convey.Convey("TestStartCommit", t, func () {
		commitChanName := "commit_1"
		sCommitChanName := "s" + commitChanName
		cCommitChanName := "c" + commitChanName
		convey.Convey("TestStartCommitPublish1Faild", func () {
			err := errors.New("Panic Test")
			db, mock := redismock.NewClientMock()
			mock.ExpectPublish(sCommitChanName, "0").SetErr(err)
			convey.So(func () {
				startCommit(db, commitChanName, func() {})
			}, convey.ShouldPanicWith, err)
		})

		convey.Convey("TestStartCommitSuccess", func () {
			mr, err := miniredis.Run()
			if err != nil {
				panic(err)
			}
			defer mr.Close()
			
			rdb := redis.NewClient(&redis.Options{
				Addr: mr.Addr(),
			})
			sCommit := rdb.Subscribe(context.TODO(), sCommitChanName)
			sCommitChan := sCommit.Channel()

			go func() {
				msg := <- sCommitChan
				println(msg)
				// convey.So(msg.Payload, convey.ShouldEqual, "0")
				rdb.Publish(context.TODO(), cCommitChanName, "0")
				
				msg = <- sCommitChan
				println(msg)
				// convey.So(msg.Payload, convey.ShouldEqual, "1")
				rdb.Publish(context.TODO(), cCommitChanName, "1")

				msg = <- sCommitChan
				println(msg)
				// convey.So(msg.Payload, convey.ShouldEqual, "2")
			}()
			convey.So(func() {
				startCommit(rdb, commitChanName, func(){})
			},convey.ShouldNotPanic)
		})
	})
}

func TestMain(t *testing.T) {
	convey.Convey("TestMainParser", t, func () {
		err := errors.New("Panic Test")
		flagsParseStub := gomonkey.ApplyFunc(flags.Parse ,func (opt interface{}) ([]string, error)  {
			return []string {"123"}, err
		})

		defer flagsParseStub.Reset()

		convey.So(func ()  {
			main()
		}, convey.ShouldPanicWith, err)
	})

	convey.Convey("TestMainPing", t, func () {
		flagsParseStub := gomonkey.ApplyFunc(flags.Parse ,func (fopt interface{}) ([]string, error)  {
			return [] string {}, nil
		})
		defer flagsParseStub.Reset()

		mr, err := miniredis.Run()

		if err != nil {
			panic(err)
		}

		optAddrStub := gomonkey.ApplyGlobalVar(&opt.Addr, mr.Host())
		defer optAddrStub.Reset()

		mrPort, _ := strconv.Atoi(mr.Port())

		optPortStub := gomonkey.ApplyGlobalVar(&opt.Port, mrPort)
		defer optPortStub.Reset()

		lazySortStub := gomonkey.ApplyFunc(lazysort.NewLazySort, func (_ string, _ int) *lazysort.LazySort {
			rd := new(lazysort.LazySort)
			return rd
		})
		defer lazySortStub.Reset()

		mr.SetError("Panic Ping Test")

		convey.So(func ()  {
			main()
		}, convey.ShouldPanicWith, "Panic Ping Test")
	})

	convey.Convey("TestMainSuccessful", t, func () {
		flagsParseStub := gomonkey.ApplyFunc(flags.Parse ,func (fopt interface{}) ([]string, error)  {
			return [] string {}, nil
		})
		defer flagsParseStub.Reset()

		mr, err := miniredis.Run()

		if err != nil {
			panic(err)
		}

		optAddrStub := gomonkey.ApplyGlobalVar(&opt.Addr, mr.Host())
		defer optAddrStub.Reset()

		mrPort, _ := strconv.Atoi(mr.Port())

		optPortStub := gomonkey.ApplyGlobalVar(&opt.Port, mrPort)
		defer optPortStub.Reset()

		lazySortStub := gomonkey.ApplyFunc(lazysort.NewLazySort, func (_ string, _ int) *lazysort.LazySort {
			rd := new(lazysort.LazySort)
			return rd
		})
		defer lazySortStub.Reset()

		startCommitStub := gomonkey.ApplyFunc(startCommit, func (rdb *redis.Client, _ string, _ func ()) {})
		defer startCommitStub.Reset()

		initRedisListStub := gomonkey.ApplyFunc(initRedisList, func (rdb *redis.Client, redisDataList string, redisFlagList string, redisWatchFlag string) {})
		defer initRedisListStub.Reset()

		signalCatchStub := gomonkey.ApplyFunc(signalCatch, func (signalChan chan os.Signal)  {})
		defer signalCatchStub.Reset()

		sendDataToRedisStub := gomonkey.ApplyFunc(sendDataToRedis, func (fileData chan *big.Int, rdb *redis.Client, redisDataList string, redisFlagList string)  {})
		defer sendDataToRedisStub.Reset()

		convey.So(func ()  {
			main()
		}, convey.ShouldNotPanic)
	})
}