package main

import (
	"fmt"
	"math/rand"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	fmt.Println("Starting Kafka producer")

	conf := &kafka.ConfigMap{
		"bootstrap.servers":   "pkc-4nxnd.asia-east2.gcp.confluent.cloud:9092",
		"sasl.mechanism":      "PLAIN",
		"security.protocol":   "SASL_SSL",
		"sasl.username":       "EG53ND5XQWLVNDMU",
		"sasl.password":       "VahdBsIPwTyi9pFlfv0V9mUQq2eNVOZjFVUapbOJgom0PRPG1/m1UDnH5vyBufqx",
		"api.version.request": true,
	}

	topic := "purchases"
	p, err := kafka.NewProducer(conf)

	if err != nil {
		fmt.Printf("Failed to create producer: %s", err)
		os.Exit(1)
	}

	// Go-routine to handle message delivery reports and
	// possibly other event types (errors, stats, etc)
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Failed to deliver message: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Produced event to topic %s: key = %-10s value = %s\n",
						*ev.TopicPartition.Topic, string(ev.Key), string(ev.Value))
				}
			}
		}
	}()

	users := [...]string{"moojig"}
	items := [...]string{"llra"}

	for n := 0; n < 10; n++ {
		key := users[rand.Intn(len(users))]
		data := items[rand.Intn(len(items))]
		p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Key:            []byte(key),
			Value:          []byte(data),
		}, nil)
	}

	// Wait for all messages to be delivered
	p.Flush(15 * 1000)
	p.Close()
}
