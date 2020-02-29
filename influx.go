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

const MyDB = "mydb"

//Put is saves points to database
func influxPut(influxClient client.Client, redisClient *redis.Client, msg []byte, topic string) {

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
		log.Println("json unmarshall error : ", err)
	}

	// "m" 에 해당하는 부분만 다른 map(sel)으로
	mSubMap, ok := dat["m"].(map[string]interface{})
	if !ok {
		fmt.Println("m in jsonMap notfound")
		return
	}
	keys := make([]string, 0, len(mSubMap))
	// "m" 의 key 부분을 추출
	for k := range mSubMap {
		keys = append(keys, k)
	}

	// mqtt topc에서 siteid를 짤라내서 s로 저장
	siteID := strings.Split(topic, "/")[1]
	dat["s"] = siteID

	// mqtt 메시지의 "i"는 태그로 그외는 필드로 저장
	equipID, ok := dat["i"].(string)
	if !ok {
		fmt.Println("i in dat notfound")
		return
	}
	tags := map[string]string{"i": equipID}
	temptm, ok := dat["t"].(float64)
	if !ok {
		fmt.Println("t in dat notfound")
		return
	}
	tm := int64(temptm)
	delete(dat, "m")
	delete(dat, "i")
	delete(dat, "t")
	for i, key := range keys {
		val := mSubMap[key]

		curVal := int(val.(float64))
		tags["k"] = key
		dat["m"] = curVal

		// redis에서 이장치의 모델을 가져온다.
		modelID, err := redisClient.Get(equipID).Result()
		if err != nil {
			log.Println(err)
		}
		fmt.Println("model id is ->[", modelID, "]")
		if modelID != "" {
			// 에러나 범위를 벗어난 값을 미리 설정된 모델의 값과 비교하여 이벤트 처리
			checkEvent(redisClient, siteID, modelID, key, curVal, equipID)
			// 여기서는 끝이지만 이벤트 서버에서 위 메시지를 sub하여 관련 처리를 함.
		}

		// i는 for loop index로 같은 시간에 온 데이터를 시간을 달리하기 위해 증가시킴
		time1 := (tm + int64(i)) * 1000000
		pt, err := client.NewPoint("iotdata5", tags, dat, time.Unix(0, time1))
		if err != nil {
			log.Fatal(err)
		}
		bp.AddPoint(pt)
	}
	// Write the batch
	if err := influxClient.Write(bp); err != nil {
		log.Fatal(err)
	}
}

// redis에서 모델아이디로 에러나 범위체크 값을 가져와서 이벤트 발생 여부를 체크하고 이벤트 발생시 redis에 publish
func checkEvent(redisClient *redis.Client, siteID string, modelID string, key string, curVal int, equipID string) {
	// redis에서 이장치의 모델에 해당하는 ov, un의 설정값을 가져온다.
	if key == "err" {
		key = "e"
	}
	tempokey, err := redisClient.SMembers(modelID + "_" + key).Result()
	if err != nil {
		fmt.Println(err)
		return
	}
	if len(tempokey) == 0 {
		return
	}

	fmt.Println("limitValue ->", tempokey)

	// 설정값이 있으면
	if key == "e" { // err
		for _, tempoValue := range tempokey {
			limitValue := strings.Split(tempoValue, ",")
			checkValue, err := strconv.Atoi(limitValue[0])
			if err != nil {
				fmt.Println(err)
			}
			if curVal == checkValue {
				tempoValue = "err," + tempoValue
				if err = redPublish(redisClient, siteID, modelID, equipID, key, curVal, tempoValue); err != nil {
					fmt.Println(err)
				}
			}
		}
	} else { // ov or un
		for _, tempoValue := range tempokey {
			limitValue := strings.Split(tempoValue, ",")
			checkValue, err := strconv.Atoi(limitValue[1])
			if err != nil {
				fmt.Println(err)
			}
			if limitValue[0] == "ov" { // over
				if checkValue < curVal { // 비교해서 넘치면 레디스에 publish함.
					if err = redPublish(redisClient, siteID, modelID, equipID, key, curVal, tempoValue); err != nil {
						fmt.Println(err)
					}
				}
			} else { // under
				if checkValue > curVal { // 모자란 경우만 레디스에 publish
					if err = redPublish(redisClient, siteID, modelID, equipID, key, curVal, tempoValue); err != nil {
						fmt.Println(err)
					}
				}
			}
		}
	}
}

func redPublish(redisClient *redis.Client, siteID string, modelID string, equipID string, key string, curVal int, tempoValue string) error {
	ocmd, err := redisClient.Publish("primEvent", siteID+","+modelID+","+equipID+","+key+","+strconv.Itoa(curVal)+","+tempoValue).Result()
	if err != nil {
		fmt.Println("reids publish err ->", err, ocmd)
		return err
	}
	return nil
}
