package main

import (
	"log"
	"math"
	"math/rand"
	"time"

	"github.com/SeongUgKim/toll-calculator/types"
	"github.com/gorilla/websocket"
)

const (
	OBUNUMS    = 20
	WSENDPOINT = "ws://127.0.0.1:30000/ws"
)

var sendInterval = time.Second

func genLatLong() (float64, float64) {
	return genCoord(), genCoord()
}

func genCoord() float64 {
	n := float64(rand.Intn(100) + 1)
	f := rand.Float64()
	return n + f
}

func generateOBUIDS(n int) []int {
	ids := make([]int, n)
	for i := 0; i < n; i++ {
		ids[i] = rand.Intn(math.MaxInt)
	}
	return ids
}

func init() {
	rand.New(rand.NewSource(time.Now().UnixNano()))
}

func main() {
	obuIDS := generateOBUIDS(OBUNUMS)
	conn, _, err := websocket.DefaultDialer.Dial(WSENDPOINT, nil)
	if err != nil {
		log.Fatal(err)
	}

	for {
		for i := 0; i < len(obuIDS); i++ {
			lat, long := genLatLong()
			data := types.OBUData{
				OBUID: obuIDS[i],
				Lat:   lat,
				Long:  long,
			}
			if err := conn.WriteJSON(data); err != nil {
				log.Fatal(err)
			}
		}
		time.Sleep(sendInterval)
	}
}
