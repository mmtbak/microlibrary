package mq

import (
	"log/slog"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/IBM/sarama/mocks"
	"github.com/mmtbak/microlibrary/config"
	"gopkg.in/go-playground/assert.v1"
)

func TestKafkaSyncSchema(t *testing.T) {
	accessconfig := config.AccessPoint{
		Source: "kafka://localhost:9092/?" +
			"topics=my-event-test-topic" +
			"&numpartition=2&numreplica=1&autocommitsecond=1" +
			"initial=oldest&version=1.1.1&clientid=microlibrary-kafka-client",
	}
	kafkamq, err := NewKafkaMessageQueue(accessconfig)
	assert.Equal(t, err, nil)
	err = kafkamq.SyncSchema()
	assert.Equal(t, err, nil)
}

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
