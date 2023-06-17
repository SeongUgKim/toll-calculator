package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/SeongUgKim/toll-calculator/types"
	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

type DataReceiver struct {
	msg  chan types.OBUData
	conn *websocket.Conn
	prod DataProducer
}

func NewDataReceiver() (*DataReceiver, error) {
	var (
		prod       DataProducer
		err        error
		kafkaTopic = "obudata"
	)

	prod, err = NewKafkaProducer(kafkaTopic)
	if err != nil {
		return nil, err
	}

	prod = NewLogMiddleware(prod)

	return &DataReceiver{
		msg:  make(chan types.OBUData, 128),
		prod: prod,
	}, nil
}

func (d *DataReceiver) produceData(data types.OBUData) error {
	return d.prod.ProduceData(data)
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
