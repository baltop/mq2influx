//mqtt 메시지를 inluxDB에 저장
//
//

package main

import (
	"encoding/json"
	"log"
	"strings"
	"time"

	_ "github.com/influxdata/influxdb1-client"
	client "github.com/influxdata/influxdb1-client/v2"
)

const (
	// MyDB specifies name of database
	MyDB = "mydb"
)

// Insert saves points to database
func Insert(productMeasurement map[string]interface{}) {
	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr: "http://192.168.0.1:8086",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	// Create a new point batch
	bp, err := client.NewBatchPoints(client.BatchPointsConfig{
		Database:  MyDB,
		Precision: "s",
	})
	if err != nil {
		log.Fatal(err)
	}

	// Create a point and add to batch
	tags := map[string]string{"productView": productMeasurement["ProductName"].(string)}
	fields := productMeasurement

	pt, err := client.NewPoint("products", tags, fields, time.Now())
	if err != nil {
		log.Fatal(err)
	}
	bp.AddPoint(pt)

	// Write the batch
	if err := c.Write(bp); err != nil {
		log.Fatal(err)
	}

	// Close client resources
	if err := c.Close(); err != nil {
		log.Fatal(err)
	}
}

//Put is saves points to database
func influxPut(influxClient client.Client, msg []byte, topic string) {

	// Create a new point batch
	bp, err := client.NewBatchPoints(client.BatchPointsConfig{
		Database:  MyDB,
		Precision: "ns",
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
	dat["s"] = strings.Split(topic, "/")[1]

	tags := map[string]string{"i": dat["i"].(string)}
	tm := int64(dat["t"].(float64))
	delete(dat, "m")
	delete(dat, "i")
	delete(dat, "t")
	for i, key := range keys {

		val := sel[key]
		dat["k"] = key
		dat["m"] = val

		// Create a point and add to batch

		fields := dat
		// time1 := tm * 10000
		time1 := tm + int64(i)

		pt, err := client.NewPoint("iotdata5", tags, fields, time.Unix(0, time1))
		if err != nil {
			log.Fatal(err)
		}
		bp.AddPoint(pt)
		//fmt.Println("influ ", tags, fields, tm)

	}
	// Write the batch
	if err := influxClient.Write(bp); err != nil {
		log.Fatal(err)
	}

}

// queryDB convenience function to query the database
func queryDB(cmd string) (res []client.Result, err error) {
	q := client.Query{
		Command:  cmd,
		Database: MyDB,
	}
	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr: "http://192.168.0.101:8086",
	})
	if response, err := c.Query(q); err == nil {
		if response.Error() != nil {
			return res, response.Error()
		}
		res = response.Results
	} else {
		return res, err
	}
	return res, nil
}
