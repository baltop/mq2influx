//mqtt broker에서 데이터를 subscribe하여 influx db에 저장.
//2020.01에 수정하여 LPWA 프로젝트에 맞게 수정함.

package main

import (
	"fmt"
	"log"
	"runtime"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/gocql/gocql"

	_ "github.com/influxdata/influxdb1-client"
	client "github.com/influxdata/influxdb1-client/v2"

	"github.com/go-redis/redis/v7"
)

// 장치별 메시지가 도착하는지를 감시
var checkLaterncy map[string]int64

// 메시지 도착 하는지 감시하는 시간 간격
var adjustTTL int64

var mux sync.RWMutex

func messageHandler(c mqtt.Client, msg mqtt.Message, session *gocql.Session, influxClient client.Client) {

	go influxPut(influxClient, msg.Payload(), msg.Topic())
}

func connLostHandler(c mqtt.Client, err error) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Recovered in connLostHandler", r)
		}
	}()
	fmt.Printf("Connection lost, reason: %v\n", err)
ERROR1:
	if token := c.Connect(); token.Wait() && token.Error() != nil {
		time.Sleep(20 * time.Second)
		goto ERROR1
	}
	fmt.Printf("reconnect!!")
}

func main() {
	// 모든 cpu를 다 사용함.
	runtime.GOMAXPROCS(runtime.NumCPU())
	// 메시지 도착 간격 감시
	checkLaterncy = make(map[string]int64)
	// 감시 간격은 11분
	adjustTTL = int64(3 * 60)
	for {
		runJob()
		time.Sleep(600 * time.Second)
		fmt.Println("restart......")
	}
}

func runJob() {

	// panic 발생시 리커버하고 메인으로 돌아감.
	defer func() {
		fmt.Println("runJob defer function called.")
		if r := recover(); r != nil {
			fmt.Println("Recovered in runJob", r)
		}
	}()

	// @TODO 컨피그파일로 배출  192.168.100.153     192.168.0.14
	cluster := gocql.NewCluster("192.168.0.101")

	cluster.Keyspace = "sensordb"
	session, err := cluster.CreateSession()
	if err != nil {
		log.Println(err)
	}
	defer session.Close()

	influxClient, err := client.NewHTTPClient(client.HTTPConfig{
		Addr: "http://192.168.0.101:8086",
	})
	if err != nil {
		log.Println(err)
	}
	defer influxClient.Close()

	redisClient := redis.NewClient(&redis.Options{
		Addr:     "192.168.0.102:16379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	pong, err := redisClient.Ping().Result()
	log.Println(pong, err)

	opts := mqtt.NewClientOptions().
		AddBroker("tcp://192.168.0.101:1883").
		SetClientID("group-one").
		SetDefaultPublishHandler(func(c mqtt.Client, msg mqtt.Message) {
			// call back 함수에 세션을 넘겨 주기 위해서 anonymous func를 만들고 session을 넘겨준다.
			messageHandler(c, msg, session, influxClient)
		}).
		SetConnectionLostHandler(connLostHandler)

	//set OnConnect handler as anonymous function
	//after connected, subscribe to topic
	opts.OnConnect = func(c mqtt.Client) {
		fmt.Printf("Client connected, subscribing to: rmms/*\n")

		//Subscribe here, otherwise after connection lost,
		//you may not receive any message
		if token := c.Subscribe("cp/#", 0, func(c mqtt.Client, msg mqtt.Message) {
			// call back 함수에 세션을 넘겨 주기 위해서 anonymous func를 만들고 session을 넘겨준다.
			messageHandler(c, msg, session, influxClient)
		}); token.Wait() && token.Error() != nil {
			fmt.Println(token.Error())
		}
	}

	var wg sync.WaitGroup
	wg.Add(1)

	//create and start a client using the above ClientOptions
	c := mqtt.NewClient(opts)
	if token := c.Connect(); token.Wait() && token.Error() != nil {
		log.Println(token.Error())
	}

	go func() {
		var lateDevices []string
		var checkPoint int64 //가장 최근의 에러처리 시점.
		var checkPointBase int64

		// 1분에 한번씩 돌면서...
		timer := time.NewTicker(60 * time.Second)
		for range timer.C {

			now := time.Now().Unix()
			fmt.Println("++++++++++++++++++++++++++++++++++++++++++++++++++")
			fmt.Println("it's check 클러스터", now)
			tm := time.Unix(now, 0)
			fmt.Println(tm.Format("2006-01-02T15:04:05-0700"))
			fmt.Println("++++++++++++++++++++++++++++++++++++++++++++++++++")
			mux.RLock()
			for k, v := range checkLaterncy {
				fmt.Println(k, "'s value =>", v)
				vm := time.Unix(v, 0)

				// && v > checkPoint
				if v < now && v > checkPoint {
					fmt.Println("late message =>[", k, "] v =>", v, "dat =>", vm.Format("2006-01-02T15:04:05-0700"), "now =>", now)
					lateDevices = append(lateDevices, k)
					if checkPointBase < v {
						checkPointBase = v
					}
				}
			}
			mux.RUnlock()
		}
		wg.Done()
	}()

	wg.Wait()
}
