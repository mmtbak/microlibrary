package mq

import (
	"testing"
	"time"

	"github.com/IBM/sarama/mocks"
	"gopkg.in/go-playground/assert.v1"
)

func TestKafkaSendMessage(t *testing.T) {

	messasge := "message content"
	sendtime := time.Now()
	msg := []byte(messasge + sendtime.Format("2006-1-2 15:04:05"))
	topics := []string{"my-event-topic"}
	// mockproducer
	mockproducer := mocks.NewSyncProducer(t, mocks.NewTestConfig())
	mockproducer.ExpectSendMessageAndSucceed()

	kafkamq := &KafkaMessageQueue{
		producer: mockproducer,
		topics:   topics,
	}

	err := kafkamq.SendMessage(msg)
	assert.Equal(t, err, nil)
}
