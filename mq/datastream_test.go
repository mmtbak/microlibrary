package mq

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/mmtbak/microlibrary/config"
)

type MyEventA struct {
	A1 string
	A2 string
}

func (ea MyEventA) Unmarshal(data []byte) error {
	return json.Unmarshal(data, &ea)
}

func (ea MyEventA) Marshal() ([]byte, error) {

	return json.Marshal(ea)
}

func (ea MyEventA) Consume() error {

	fmt.Println(ea.A1)
	return nil
}

func TestNewMessageQueue(t *testing.T) {

	var conf = config.AccessPoint{
		Source: "kafka://tcp(127.0.0.1:9093)/?topic=my_event&",
		Options: map[string]interface{}{
			"consumergroup": "footstone",
			"numpartition":  3,
			"numreplica":    3,
		},
	}
	mq, err := NewMessageQueue(conf)
	if err != nil {
		fmt.Println(err)
		return
	}

	dataqueue := NewDataStream(mq, new(MyEventA))
	e1 := MyEventA{
		A1: "hello  ",
		A2: "world",
	}
	dataqueue.SendMessage(e1)
	dataqueue.Consume()
}
