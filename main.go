package main

import (
	"fmt"

	"github.com/sfdc-pcg/pcgh-golib-core/kafka"
)

func main() {
	appconfig := kafka.DecodeConfig()
	fmt.Printf("AppConfig: %v\n", &appconfig)

	client := kafka.NewKafkaClient(&appconfig)
	fmt.Printf("Received client as %v\n", client)
	client.ReceivedMessages = make([]kafka.Message, 0)

	topic := appconfig.Topic()
	fmt.Printf("Kinesis Topic is %v\n", topic)

	test := make(map[string]string)
	test["name"] = "kinesis test"
	test["id"] = "12345"

	go client.EventProducer(appconfig, "test_event", test)
	defer client.Producer.Close()
	defer client.Consumer.Close()
}
