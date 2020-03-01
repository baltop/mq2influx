//mqtt broker에서 게이트웨이가 보낸 센서 데이터를 subscribe하여 influx db에 저장.
//2020.01에 수정하여 LPWA 프로젝트에 맞게 수정함.
//main.go 와 influx.go

package main

import (
	"encoding/json"
	"fmt"
	"log"
	"runtime"
	"strings"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/sasbury/mini"

	_ "github.com/influxdata/influxdb1-client"
	client "github.com/influxdata/influxdb1-client/v2"

	"github.com/go-redis/redis/v7"
)

var redisserver string
var mqttserver string
var influxdb string

//mqtt로 받은 메시지 payload를 담을 struct
type mqMessage struct {
	influxClient client.Client
	redisClient  *redis.Client
	Site         string
	Time         float64            `json:"t"`
	ID           string             `json:"i"`
	Measure      map[string]float64 `json:"m"`
}

func messageHandler(msg mqtt.Message, influxClient client.Client, redisClient *redis.Client) {
	mqMsg := new(mqMessage)
	if err := json.Unmarshal(msg.Payload(), &mqMsg); err != nil {
		panic(err)
	}
	mqMsg.Site = strings.Split(msg.Topic(), "/")[1]
	mqMsg.influxClient = influxClient
	mqMsg.redisClient = redisClient

	influxPut(mqMsg)
}

func main() {
	// 모든 cpu를 다 사용함.
	runtime.GOMAXPROCS(runtime.NumCPU())

	config, err := mini.LoadConfiguration("mq2influx.ini")
	if err != nil {
		log.Fatal(err)
	}

	redisserver = config.String("redisserver", "192.168.0.102:16379")
	mqttserver = config.String("mqttserver", "tcp://192.168.0.101:1883")
	influxdb = config.String("influxdb", "http://192.168.0.101:8086")

	// for {
	runJob()
	// 주로 통신에러일 경우 서버가 재기동할때까지..
	// time.Sleep(600 * time.Second)
	fmt.Println("restart......")
	// }
}

func runJob() {
	// panic 발생시 리커버하고 메인으로 돌아감.
	defer func() {
		//fmt.Println("runJob defer function called.")
		if r := recover(); r != nil {
			fmt.Println("Recovered in runJob", r)
		}
	}()

	// influx client. 매뉴얼에 goroutine에서 사용가능하다고 함.
	influxClient, err := client.NewHTTPClient(client.HTTPConfig{
		Addr: influxdb,
	})
	if err != nil {
		panic(err)
		//log.Fatal(err)
	}
	defer influxClient.Close()

	// redis client connection pool
	redisClient := redis.NewClient(&redis.Options{
		Addr:     redisserver,
		Password: "",
		DB:       0,
	})
	pong, err := redisClient.Ping().Result()
	fmt.Println("redis connection start status", pong)
	if err != nil {
		panic(err)
		//log.Fatal(err)
	}

	// mqtt 서버 접속 옵션
	opts := mqtt.NewClientOptions().
		AddBroker(mqttserver).
		SetClientID("group-two").
		SetAutoReconnect(true).
		SetClientID("influxclient").
		SetKeepAlive(5 * time.Second).
		SetMaxReconnectInterval(20 * time.Second).
		SetConnectionLostHandler(connLostHandler)

	var wg sync.WaitGroup
	wg.Add(1)
	//after connected, subscribe to topic
	opts.OnConnect = func(c mqtt.Client) {
		if token := c.Subscribe("ms/#", 0, func(c mqtt.Client, msg mqtt.Message) {
			fmt.Println("onconnect subscribe", msg.Topic())
			go messageHandler(msg, influxClient, redisClient)
		}); token.Wait() && token.Error() != nil {
			fmt.Println("mqtt sub error ", token.Error())
			wg.Done()
		}
	}

	c := mqtt.NewClient(opts)
	if token := c.Connect(); token.Wait() && token.Error() != nil {
		log.Println("mqtt initial connect error ", token.Error())
	}

	fmt.Println("the last line-1")
	wg.Wait()
	fmt.Println("the last line")
}

// mqtt connection 재연결
func connLostHandler(c mqtt.Client, err error) {
	fmt.Printf("Connection lost, reason: %v\n", err)

}
