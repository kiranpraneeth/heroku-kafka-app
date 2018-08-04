package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"time"

	"github.com/Shopify/sarama"
	"github.com/gin-gonic/gin"
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

	go client.consumeMessages()
	defer client.producer.Close()
	defer client.consumer.Close()

	router := gin.New()
	router.Use(gin.Logger())
	router.LoadHTMLGlob("templates/*.tmpl.html")
	router.Static("/public", "public")

	router.GET("/", indexGET)
	router.GET("/messages", client.messagesGET)
	router.POST("/messages", client.messagesPOST)

	router.Run(":" + appconfig.Web.Port)

}

// This endpoint accesses in memory state gathered
// by the consumer, which holds the last 10 messages received
func (kc *KafkaClient) messagesGET(c *gin.Context) {
	kc.ml.RLock()
	defer kc.ml.RUnlock()
	c.JSON(http.StatusOK, kc.receivedMessages)

}

// A sample producer endpoint.
// It receives messages as http bodies on /messages,
// and posts them directly to a Kafka topic.
func (kc *KafkaClient) messagesPOST(c *gin.Context) {
	message, err := ioutil.ReadAll(c.Request.Body)
	fmt.Printf("Got message %v", message)
	if err != nil {
		log.Fatal(err)
	}
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.ByteEncoder("test123"),
		Value: sarama.ByteEncoder(message),
	}

	fmt.Printf("Producing message key: %v and byteencoder %v", c.Request.RemoteAddr, sarama.ByteEncoder(c.Request.RemoteAddr))
	kc.producer.Input() <- msg
}

// Consume messages from the topic and buffer the last 10 messages
// in memory so that the web app can send them back over the API.
func (kc *KafkaClient) consumeMessages() {
	fmt.Printf("Entered consumeMessages")
	for {
		select {
		case message := <-kc.consumer.Messages():
			fmt.Printf("Going through saveMessage AND GOT MESSAGE: %v", message)
			kc.saveMessage(message)
		}
	}
}

// Save consumed message in the buffer consumed by the /message API
func (kc *KafkaClient) saveMessage(msg *sarama.ConsumerMessage) {
	kc.ml.Lock()
	defer kc.ml.Unlock()
	fmt.Printf("Received Message and waiting for greater than 10")
	if len(kc.receivedMessages) >= 10 {
		kc.receivedMessages = kc.receivedMessages[1:]
	}
	fmt.Printf("Appending Message partiion: msg.Partition Offset: %v\n", msg)
	kc.receivedMessages = append(kc.receivedMessages, Message{
		Partition: msg.Partition,
		Offset:    msg.Offset,
		Value:     string(msg.Value),
		Metadata: MessageMetadata{
			ReceivedAt: time.Now(),
		},
	})
}
