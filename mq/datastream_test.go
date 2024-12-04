package mq

import (
	"encoding/json"
	"fmt"
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
