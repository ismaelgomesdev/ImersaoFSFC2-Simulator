package kafka

import (
	"encoding/json"
	"log"
	"os"
	"time"

	ckafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/ismaelgomesufc/ImersaoFSFC2-Simulator/application/route"
	"github.com/ismaelgomesufc/ImersaoFSFC2-Simulator/infra/kafka"
)

//{"clientId":"1", "routeId":"1"}
func Produce(msg *ckafka.Message) {
	producer := kafka.NewKaflaProducer()
	route := route.NewRoute()
	json.Unmarshal(msg.Value, &route)
	route.LoadPositions()
	positions, err := route.ExportJsonPositions()
	if err != nil {
		log.Println(err.Error())
	}

	for _, p := range positions {
		kafka.Publish(p, os.Getenv("KafkaProduceTopic"), producer)
		time.Sleep(time.Millisecond * 500)
	}

}
