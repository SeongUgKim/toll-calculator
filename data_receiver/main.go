package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/SeongUgKim/toll-calculator/types"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/gorilla/websocket"
)

var kafkaTopic = "obudata"

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

type DataReceiver struct {
	msg  chan types.OBUData
	conn *websocket.Conn
	prod *kafka.Producer
}

func NewDataReceiver() (*DataReceiver, error) {
	prod, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost"})
	if err != nil {
		return nil, err
	}

	// Start another goroutine to check if we have delivered the data
	go func() {
		for e := range prod.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
				}
			}
		}
	}()

	return &DataReceiver{
		msg:  make(chan types.OBUData, 128),
		prod: prod,
	}, nil
}

func (d *DataReceiver) produceData(data types.OBUData) error {
	b, err := json.Marshal(data)
	if err != nil {
		return err
	}

	return d.prod.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &kafkaTopic,
			Partition: kafka.PartitionAny,
		},
		Value: b,
	}, nil)
}

func (d *DataReceiver) handleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Fatal(err)
	}

	d.conn = conn
	go d.wsReceiveLoop()
}

func (d *DataReceiver) wsReceiveLoop() {
	fmt.Println("New OBU client connected")
	for {
		var data types.OBUData
		if err := d.conn.ReadJSON(&data); err != nil {
			log.Println("read error:", err)
			continue
		}
		if err := d.produceData(data); err != nil {
			log.Println("kafka produce error:", err)
		}
	}
}

func main() {
	recv, err := NewDataReceiver()
	if err != nil {
		log.Fatal(err)
	}

	http.HandleFunc("/ws", recv.handleWS)
	http.ListenAndServe(":30000", nil)
}
