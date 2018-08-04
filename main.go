package main

import (
	"encoding/json"
	"fmt"

	"github.com/sfdc-pcg/pcgh-golib-core/kafka"
)

type eventtestpayload struct {
	name string
	id   int64
}

func main() {
	appconfig := kafka.DecodeConfig()
	fmt.Printf("AppConfig: %v\n", &appconfig)

	client := kafka.NewKafkaClient(&appconfig)
	fmt.Printf("Received client as %v\n", client)
	client.ReceivedMessages = make([]kafka.Message, 0)

	topic := appconfig.Topic()
	fmt.Printf("Kinesis Topic is %v\n", topic)

	p := eventtestpayload{
		name: "kinesis test",
		id:   12345,
	}
	j, err := json.Marshal(p)
	if err != nil {
		fmt.Printf("Failed json marshalling\n")
	}

	go client.EventProducer(appconfig, "test_event", j)
	defer client.Producer.Close()
	defer client.Consumer.Close()
}
