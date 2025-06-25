package mq

import (
	"fmt"

	"github.com/mmtbak/dsnparser"
)

// ConsumeMessageFunc 处理消息方法，默认不应答ack.
type ConsumeMessageFunc func(Message)

// MessageQueue  消息队列接口规范.
type MessageQueue interface {
	// SyncSchema create topic
	SyncSchema() error
	// SendMessage send message
	SendMessage(b []byte, opts ...*SendMsgOption) error
	// ReceiveMessage receive message
	ReceiveMessage() (<-chan Message, error)
	// Close mq close
	Close() error
}

// Message   message content interface.
type Message interface {
	ID() string
	Body() []byte
	Ack() error
	Nack() error
}

// NewMessageQueue ...
func NewMessageQueue(source string) (MessageQueue, error) {
	var err error
	dsndata := dsnparser.Parse(source)
	schema := dsndata.GetScheme()
	switch schema {
	case "kafka":
		return NewKafkaMessageQueue(source)
	default:
		err = fmt.Errorf("mq:unsupported schema '%s'", schema)
	}
	return nil, err
}
