package mq

import (
	"fmt"

	"github.com/mmtbak/microlibrary/config"
)

// ConsumeMessageFunc 处理消息方法，默认不应答ack.
type ConsumeMessageFunc func(Message)

// MessageQueue  消息队列接口规范.
type MessageQueue interface {
	// SyncSchema create topic
	SyncSchema() error
	// SendMessage send message
	SendMessage(b []byte, opts ...SendMsgOption) error
	// ConsumeMessage , start consome message , return when failed
	ConsumeMessage(cb ConsumeMessageFunc, opts ...ConsumeMsgOption) error
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
func NewMessageQueue(conf config.AccessPoint) (MessageQueue, error) {
	dsn, err := conf.Decode(nil)
	if err != nil {
		return nil, err
	}
	switch dsn.Scheme {
	case "kafka":
		return NewKafkaMessageQueue(conf)
	default:
		err = fmt.Errorf("mq:unsupported schema '%s'", dsn.Scheme)
	}
	return nil, err
}
