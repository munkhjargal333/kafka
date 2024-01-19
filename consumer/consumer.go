package main

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Bus struct {
	ID         int      `json:"id"`
	Route      string   `json:"routeid"`
	Driver     string   `json:"driver"`
	BusId      int      `json:"busId"`
	Location   Location `json:"location"`
	SequenceId int      `json:"sequenceId"`
}

type Location struct {
	Lat float32 `json:"lat"`
	Lon float32 `json:"lon"`
}

var localBus = new(Bus)

func main() {
	conf := &kafka.ConfigMap{
		"bootstrap.servers":   "pkc-4nxnd.asia-east2.gcp.confluent.cloud:9092",
		"sasl.mechanism":      "PLAIN",
		"security.protocol":   "SASL_SSL",
		"sasl.username":       "EG53ND5XQWLVNDMU",
		"sasl.password":       "VahdBsIPwTyi9pFlfv0V9mUQq2eNVOZjFVUapbOJgom0PRPG1/m1UDnH5vyBufqx",
		"api.version.request": true,
		"group.id":            "kafka-go-getting-started",
		"auto.offset.reset":   "earliest",
	}

	c, err := kafka.NewConsumer(conf)
	if err != nil {
		fmt.Printf("Failed to create consumer: %s", err)
		os.Exit(1)
	}

	fmt.Println("starting consumer")

	defer c.Close()

	conn, err := net.Dial("tcp", "localhost:8081")
	if err != nil {
		fmt.Printf("Failed to connect to destination server: %s\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	fmt.Println("Connected to destination server")

	topic := "1"
	err = c.SubscribeTopics([]string{topic}, nil)
	if err != nil {
		fmt.Printf("Failed to subscribe to topic: %s", err)
		os.Exit(1)
	}

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	for {
		select {
		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			return
		default:
			ev, err := c.ReadMessage(100 * time.Millisecond)
			if err != nil {
				continue
			}

			loc := Location{}
			if err := json.Unmarshal(ev.Value, &loc); err != nil {
				fmt.Printf("Failed to parse location JSON: %s\n", err)
				continue
			}

			localBus.Route = *ev.TopicPartition.Topic
			localBus.SequenceId = int(ev.TopicPartition.Partition)
			localBus.Location = loc

			fmt.Printf("Received bus data: %+v\n", localBus)
			busJSON, err := json.Marshal(localBus)
			if err != nil {
				fmt.Printf("Failed to marshal bus data to JSON: %s\n", err)
				continue
			}

			// Send the JSON data over the TCP connection
			_, err = conn.Write(busJSON)
			if err != nil {
				fmt.Printf("Failed to send bus data over TCP: %s\n", err)
				continue
			}

			fmt.Println("Sent bus data over TCP")
		}
	}
}
