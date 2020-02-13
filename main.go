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

	_ "github.com/influxdata/influxdb1-client"
	client "github.com/influxdata/influxdb1-client/v2"

	"github.com/go-redis/redis/v7"
)

func messageHandler(msg mqtt.Message, influxClient client.Client, redisClient *redis.Client, allchan chan<- interface{}) {
	//fmt.Println(string(msg.Payload()))
	go influxPut(influxClient, redisClient, msg.Payload(), msg.Topic(), allchan)
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

	value, err := redisClient.HGet("foot", "cairo").Result()

	allchan := make(chan interface{}, 300)

	log.Println(value, err)
	opts := mqtt.NewClientOptions().
		AddBroker("tcp://192.168.0.101:1883").
		SetClientID("group-one").
		SetDefaultPublishHandler(func(c mqtt.Client, msg mqtt.Message) {
			// call back 함수에 세션을 넘겨 주기 위해서 anonymous func를 만들고 session을 넘겨준다.
			messageHandler(msg, influxClient, redisClient, allchan)
		}).
		SetConnectionLostHandler(connLostHandler)

	//set OnConnect handler as anonymous function
	//after connected, subscribe to topic
	opts.OnConnect = func(c mqtt.Client) {
		fmt.Printf("Client connected, subscribing to: cp/*\n")

		//Subscribe here, otherwise after connection lost,
		//you may not receive any message
		if token := c.Subscribe("cp/#", 0, func(c mqtt.Client, msg mqtt.Message) {
			// call back 함수에 세션을 넘겨 주기 위해서 anonymous func를 만들고 session을 넘겨준다.
			messageHandler(msg, influxClient, redisClient, allchan)
		}); token.Wait() && token.Error() != nil {
			fmt.Println(token.Error())
		}
	}

	go func(allchan <-chan interface{}) {
		for {
			if tags, success := <-allchan; success {
				//fmt.Println("recieved all channel ->", tags.(map[string]interface{}))
				tags.(map[string]interface{})["t"] = 0
			} else {
				break
			}
		}

	}(allchan)

	var wg sync.WaitGroup
	wg.Add(1)

	//create and start a client using the above ClientOptions
	c := mqtt.NewClient(opts)
	if token := c.Connect(); token.Wait() && token.Error() != nil {
		log.Println(token.Error())
	}

	wg.Wait()
}
