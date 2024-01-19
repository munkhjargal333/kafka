package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/gofiber/fiber/v2"
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

var producer *kafka.Producer
var consumer *kafka.Consumer
var socket net.Conn

var localBus = new(Bus)

func main() {
	app := fiber.New()

	producerCon()
	//go consumerCon()

	app.Post("/send", CreateTest)
	app.Get("/get", GetBus)
	app.Get("/", func(c *fiber.Ctx) error { return c.SendString("i'm fine") })

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigchan
		fmt.Println("Received shutdown signal. Closing producer...")
		producer.Close()
		os.Exit(0)
	}()

	log.Fatal(app.Listen(":8080"))
}

func producerCon() {
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
		fmt.Printf("Failed to create producer: %s", err)
		os.Exit(1)
	}
	fmt.Println("starting producer")
}

func CreateTest(c *fiber.Ctx) error {
	bus := new(Bus)
	if err := c.BodyParser(bus); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"message": err.Error()})
	}
	fmt.Println(bus)

	locJSON, err := json.Marshal(bus.Location)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"message": "Failed to marshal bus to JSON"})
	}

	topic := bus.Route
	partition := bus.SequenceId
	err = producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: int32(partition)},
		Value:          locJSON,
	}, nil)

	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"message": "Failed to produce message to Kafka"})
	}

	return c.SendString("done")
}

func GetBus(c *fiber.Ctx) error {
	return c.JSON(localBus)
}

/*
func SocketToKafka(conn net.Conn, producer *kafka.Producer) {
	defer conn.Close()
	fmt.Println("Accepted new connection")

	// Example: read data from the socket and produce it to Kafka
	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading from connection: %v\n", err)
		return
	}

	message := string(buf[:n])
	fmt.Printf("Received message: %s\n", message)

	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to produce message to Kafka: %v\n", err)
		return
	}

	fmt.Println("Message produced to Kafka")
}

func consumerCon() {
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
*/

// func KafkaToSocket(conn net.Conn, producer *kafka.Consumer) {
// 	defer conn.Close()
// 	fmt.Println("Accepted kafka to socket")
// 	buf := make([]byte, 1024)

// 	conn, err := net.Dial("tcp", "http://localhost:8081/")
// 	if err != nil {
// 		fmt.Printf("Failed to connect to destination server: %s\n", err)
// 		os.Exit(1)
// 	}
// 	defer conn.Close()

// 	fmt.Println("Connected to destination server")
// }

/*
func socketCon() {
	var err error
	listener, err := net.Listen("tcp", "localhost:8081")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to start socket server: %v\n", err)
		os.Exit(1)
	}
	defer listener.Close()

	fmt.Println("Socket server listening on localhost:8081")

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error accepting connection: %v\n", err)
			continue
		}
		//go handleConnection(conn, consumer)
		socket = conn
	}
}
*/
