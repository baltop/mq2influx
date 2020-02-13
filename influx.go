//mqtt 메시지를 inluxDB에 저장
//
//

package main

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis/v7"
	_ "github.com/influxdata/influxdb1-client"
	client "github.com/influxdata/influxdb1-client/v2"
)

const (
	// MyDB specifies name of database
	MyDB = "mydb"
)

// Insert saves points to database
// func Insert(productMeasurement map[string]interface{}) {
// 	c, err := client.NewHTTPClient(client.HTTPConfig{
// 		Addr: "http://192.168.0.1:8086",
// 	})
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// 	defer c.Close()

// 	// Create a new point batch
// 	bp, err := client.NewBatchPoints(client.BatchPointsConfig{
// 		Database:  MyDB,
// 		Precision: "s",
// 	})
// 	if err != nil {
// 		log.Fatal(err)
// 	}

// 	// Create a point and add to batch
// 	tags := map[string]string{"productView": productMeasurement["ProductName"].(string)}
// 	fields := productMeasurement

// 	pt, err := client.NewPoint("products", tags, fields, time.Now())
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// 	bp.AddPoint(pt)

// 	// Write the batch
// 	if err := c.Write(bp); err != nil {
// 		log.Fatal(err)
// 	}

// 	// Close client resources
// 	if err := c.Close(); err != nil {
// 		log.Fatal(err)
// 	}
// }

//Put is saves points to database
func influxPut(influxClient client.Client, redisClient *redis.Client, msg []byte, topic string, allchan chan<- interface{}) {

	// Create a new point batch
	bp, err := client.NewBatchPoints(client.BatchPointsConfig{
		Database:  MyDB,
		Precision: "ms",
	})
	if err != nil {
		log.Fatal(err)
	}

	var dat map[string]interface{}
	if err := json.Unmarshal(msg, &dat); err != nil {
		panic(err)
	}

	// msr에서 해당하는 부분만 다른 map(sel)으로
	sel := dat["m"].(map[string]interface{})
	keys := make([]string, len(sel))
	// msr의 키부분을 추출
	i := 0
	for k := range sel {
		keys[i] = k
		i++
	}

	// mqtt topc에서 siteid를 짤라내서 s로 저장
	dat["s"] = strings.Split(topic, "/")[1]

	// mqtt 메시지의 "i"는 태그로 그외는 필드로 저장
	equipID := dat["i"].(string)
	tags := map[string]string{"i": equipID}
	tm := int64(dat["t"].(float64))
	delete(dat, "m")
	delete(dat, "i")
	delete(dat, "t")
	for i, key := range keys {
		val := sel[key]

		curVal := int(val.(float64))
		tags["k"] = key
		dat["m"] = curVal

		tempokey, err := redisClient.SMembers(equipID + "_" + key).Result()
		if err != nil {
			fmt.Println(err)
		}
		if len(tempokey) > 0 {
			fmt.Println("limitValue ->", tempokey)
			for _, tempoValue := range tempokey {
				limitValue := strings.Split(tempoValue, ",")
				checkValue, err := strconv.Atoi(limitValue[1])
				if err != nil {
					fmt.Println(err)
				}
				if limitValue[0] == "ov" { // over
					if checkValue < curVal {
						if err = redPublish(redisClient, equipID, key, curVal, tempoValue); err != nil {
							fmt.Println(err)
						}
					}
				} else { // under
					if checkValue > curVal {
						if err = redPublish(redisClient, equipID, key, curVal, tempoValue); err != nil {
							fmt.Println(err)
						}
					}
				}
			}
		}

		// redis에서 i + key로 get 상한값, 하한값
		// key가 "err" 이면 err code

		// i + key -> maxlmt < value 가 크면 maxlmt_act 를 redis에서 가져옴.

		// value, err := redisClient.HGetAll("foot").Result()
		// if err != nil {
		// 	log.Println(err)
		// }
		// fmt.Println(value)
		// fmt.Println(value["cairo"])
		//fmt.Printf("type is  %T", value["height"])

		// 여기서는 끝이지만 이벤트 서버에서 위 메시지를 sub하여 관련 처리를 함.

		// 이벤트 서버에서 noti와 command를 처리함.

		// influx에 넣을 데이터 만들기

		// redis pub에 i + key + maxlmt + value 를 보냄

		// i는 for loop index로 같은 시간에 온 데이터를 시간을 달리하기 위해 증가시킴
		//fmt.Println(tm)
		//fmt.Println(tm + int64(i))
		time1 := (tm + int64(i)) * 1000000
		// fmt.Println(time1)
		pt, err := client.NewPoint("iotdata5", tags, dat, time.Unix(0, time1))
		if err != nil {
			log.Fatal(err)
		}
		bp.AddPoint(pt)
		//fmt.Println("influ ", tags, dat, tm)
		sendata := make(map[string]interface{})
		for k, v := range tags {
			sendata[k] = v
		}
		for k, v := range dat {
			sendata[k] = v
		}
		sendata["t"] = tm
		allchan <- sendata

	}
	// Write the batch
	if err := influxClient.Write(bp); err != nil {
		log.Fatal(err)
	}

}

func redPublish(redisClient *redis.Client, equipID string, key string, curVal int, tempoValue string) error {
	ocmd, err := redisClient.Publish("primEvent", equipID+", "+key+", "+strconv.Itoa(curVal)+", "+tempoValue).Result()
	if err != nil {
		fmt.Println("reids publish err ->", err, ocmd)
		return err
	}
	return nil
}

// queryDB convenience function to query the database
// func queryDB(cmd string) (res []client.Result, err error) {
// 	q := client.Query{
// 		Command:  cmd,
// 		Database: MyDB,
// 	}
// 	c, err := client.NewHTTPClient(client.HTTPConfig{
// 		Addr: "http://192.168.0.101:8086",
// 	})
// 	if response, err := c.Query(q); err == nil {
// 		if response.Error() != nil {
// 			return res, response.Error()
// 		}
// 		res = response.Results
// 	} else {
// 		return res, err
// 	}
// 	return res, nil
// }
