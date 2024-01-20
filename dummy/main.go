package main

import (
	"confluent/model"
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"strconv"
	"time"
)

// DataGenerator defines the interface for generating dummy data
type DataGenerator interface {
	Generate() []byte
}

// LocationDataGenerator generates dummy location data with latitude and longitude
type LocationDataGenerator struct{}

// Generate generates dummy location data with latitude and longitude
func (g LocationDataGenerator) Generate() []byte {
	// Generate random values for the fields (for demonstration purposes, using simple logic)
	id := rand.Intn(100) + 1
	route := "1" //strconv.Itoa(rand.Intn(1000))
	driver := "Driver" + strconv.Itoa(rand.Intn(100))
	busID := rand.Intn(1000) + 1
	latitude := rand.Float32()*180.0 - 90.0
	longitude := rand.Float32()*360.0 - 180.0
	sequenceID := rand.Intn(10) + 1

	// Create a Bus instance with random data
	bus := model.Bus{
		ID:         id,
		Route:      route,
		Driver:     driver,
		BusId:      busID,
		Location:   model.Location{Lat: latitude, Lon: longitude},
		SequenceId: sequenceID,
	}

	jsonData, err := json.Marshal(bus)
	if err != nil {
		fmt.Println("Error marshaling JSON:", err.Error())
		return nil
	}

	return jsonData
}

func main() {
	for {
		conn, err := net.Dial("tcp", "localhost:9090")
		if err != nil {
			fmt.Println("Error connecting to server:", err.Error())
			// Wait for a short duration before attempting to reconnect
			time.Sleep(1 * time.Second)
			continue
		}

		defer conn.Close() // Ensure the connection is closed when main() exits

		fmt.Println("Connected to server on :9090")

		dataGenerator := LocationDataGenerator{}

		data := dataGenerator.Generate()

		// Write data to the server
		_, err = conn.Write(data)
		if err != nil {
			fmt.Println("Error writing to server:", err.Error())
			break
		}

		// Print the sent message
		fmt.Println("Sent message:", string(data))

		// Wait for a short duration before sending the next message
		time.Sleep(1 * time.Second)
	}
}
