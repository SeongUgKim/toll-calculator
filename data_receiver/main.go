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
}

func NewDataReceiver() *DataReceiver {
	return &DataReceiver{
		msg: make(chan types.OBUData, 128),
	}
}

func (d *DataReceiver) handleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Fatal(err)
	}

	defer conn.Close()
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
		fmt.Printf("received OBU data from [%d] :: <lat %.2f, long %.2f>\n", data.OBUID, data.Lat, data.Long)
		d.msg <- data
	}
}

func main() {
	recv := NewDataReceiver()
	http.HandleFunc("/ws", recv.handleWS)
	http.ListenAndServe(":30000", nil)
}
