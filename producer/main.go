package main

import (
	"confluent/model"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var producer *kafka.Producer

func main() {
	// Initialize Kafka producer
	if err := initProducer(); err != nil {
		fmt.Printf("Failed to initialize Kafka producer: %s\n", err)
		os.Exit(1)
	}
	defer producer.Close()

	// Start listening for incoming connections
	listenAndServe()
}

func listenAndServe() {
	listener, err := net.Listen("tcp", "localhost:9090")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to start socket server: %v\n", err)
		os.Exit(1)
	}
	defer listener.Close()

	fmt.Println("Socket server listening on localhost:9090")

	// Handle OS signals for graceful shutdown
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigchan
		fmt.Println("Shutting down...")
		os.Exit(0)
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error accepting connection: %v\n", err)
			continue
		}
		// Handle each incoming connection in a separate goroutine
		go handleRequest(conn)
	}
}

func handleRequest(conn net.Conn) {
	defer conn.Close()

	decoder := json.NewDecoder(conn)

	var bus model.Bus

	if err := decoder.Decode(&bus); err != nil {
		fmt.Println("Error decoding JSON:", err)
		return
	}

	fmt.Printf("Received message: %+v\n", bus)

	locJson, err := json.Marshal(bus.Location)
	if err != nil {
		fmt.Println("Error marshaling JSON:", err)
		return
	}

	topic := bus.Route
	partition := bus.SequenceId
	err = producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: int32(partition)},
		Value:          locJson,
	}, nil)
	if err != nil {
		fmt.Println("Error producing message to Kafka:", err)
	}
}

func initProducer() error {
	conf := &kafka.ConfigMap{
		"bootstrap.servers":   "pkc-4nxnd.asia-east2.gcp.confluent.cloud:9092",
		"sasl.mechanism":      "PLAIN",
		"security.protocol":   "SASL_SSL",
		"sasl.username":       "EG53ND5XQWLVNDMU",
		"sasl.password":       "VahdBsIPwTyi9pFlfv0V9mUQq2eNVOZjFVUapbOJgom0PRPG1/m1UDnH5vyBufqx",
		"api.version.request": true,
	}

	var err error
	producer, err = kafka.NewProducer(conf)
	if err != nil {
		return fmt.Errorf("Failed to create Kafka producer: %s", err)
	}
	fmt.Println("Kafka producer started")
	return nil
}
