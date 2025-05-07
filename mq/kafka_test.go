package mq

import (
	"log/slog"
	"testing"
	"time"

	"github.com/IBM/sarama"
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
		config: &KafkaConfig{
			Topics:           topics,
			ConsumerGroup:    "my-event-group",
			NumOfPartition:   3,
			NumOfReplica:     2,
			AutoCommitSecond: 1,
			BufferSize:       1024,
			Initial:          sarama.OffsetNewest,
			Version:          sarama.V1_1_1_0,
			ClientID:         "microlibrary-kafka-client",
		},
		hosts:  []string{"localhost:9092"},
		logger: slog.Default(),
	}
	err := kafkamq.SendMessage(msg)
	assert.Equal(t, err, nil)
}
