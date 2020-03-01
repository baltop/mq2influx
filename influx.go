//mqtt 메시지를 inluxDB에 저장
//
//

package main

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	_ "github.com/influxdata/influxdb1-client"
	client "github.com/influxdata/influxdb1-client/v2"
)

const MyDB = "mydb"

//Put is saves points to database
func influxPut(mqMsg *mqMessage) {

	// Create a new point batch
	bp, err := client.NewBatchPoints(client.BatchPointsConfig{
		Database:  MyDB,
		Precision: "ms",
	})
	if err != nil {
		log.Fatal(err)
	}

	keys := make([]string, 0, len(mqMsg.Measure))
	for k := range mqMsg.Measure {
		keys = append(keys, k)
	}

	tags := make(map[string]string, 3)
	tags["i"] = mqMsg.ID
	tags["s"] = mqMsg.Site

	tm := int64(mqMsg.Time)
	fields := make(map[string]interface{}, 1)

	for i, key := range keys {
		val := mqMsg.Measure[key]
		tags["k"] = key

		fields["m"] = int(val)

		// redis에서 이장치의 모델을 가져온다.
		modelID, _ := mqMsg.redisClient.Get(mqMsg.ID).Result()

		if modelID != "" {
			fmt.Println("model id is ->[", modelID, "]")
			// 에러나 범위를 벗어난 값을 미리 설정된 모델의 값과 비교하여 이벤트 처리
			checkEvent(mqMsg, modelID, key, int(val))
			// 여기서는 끝이지만 이벤트 서버에서 위 메시지를 sub하여 관련 처리를 함.
		}

		// i는 for loop index로 같은 시간에 온 데이터를 시간을 달리하기 위해 증가시킴
		time1 := (tm + int64(i)) * 1000000
		pt, err := client.NewPoint("iotdata5", tags, fields, time.Unix(0, time1))
		if err != nil {
			log.Fatal(err)
		}
		bp.AddPoint(pt)
	}
	// Write the batch
	if err := mqMsg.influxClient.Write(bp); err != nil {
		log.Fatal(err)
	}
}

// redis에서 모델아이디로 에러나 범위체크 값을 가져와서 이벤트 발생 여부를 체크하고 이벤트 발생시 redis에 publish
func checkEvent(mqMsg *mqMessage, modelID string, key string, curVal int) {
	// redis에서 이장치의 모델에 해당하는 ov, un의 설정값을 가져온다.
	if key == "err" {
		key = "e"
	}
	tempokey, err := mqMsg.redisClient.SMembers(modelID + "_" + key).Result()
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
				if err = redPublish(mqMsg, modelID, key, curVal, tempoValue); err != nil {
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
			switch {
			case limitValue[0] == "ov" && checkValue < curVal:
				fallthrough
			case limitValue[0] == "un" && checkValue > curVal:
				if err = redPublish(mqMsg, modelID, key, curVal, tempoValue); err != nil {
					fmt.Println(err)
				}
			}

		}
	}
}

func redPublish(mqMsg *mqMessage, modelID string, key string, curVal int, tempoValue string) error {
	ocmd, err := mqMsg.redisClient.Publish("primEvent", mqMsg.Site+","+modelID+","+mqMsg.ID+","+key+","+strconv.Itoa(curVal)+","+tempoValue).Result()
	if err != nil {
		fmt.Println("reids publish err ->", err, ocmd)
		return err
	}
	return nil
}
