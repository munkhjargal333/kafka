package model

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
