package mq

import (
	"log"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/IBM/sarama/mocks"
	"gopkg.in/go-playground/assert.v1"
)

func TestKafkaSyncSchema(t *testing.T) {
	sarama.Logger = log.New(os.Stdout, "[Sarama] ", log.LstdFlags)
	kafkaSource := "kafka://127.0.0.1:9092/?" +
		"topics=my-event-test-topic" +
		"&numpartition=2&numreplica=1&autocommitsecond=1" +
		"initial=oldest&clientid=microlibrary-kafka-client"
	slog.Info("connecting to kafka")
	kafkamq, err := NewKafkaMessageQueue(kafkaSource)
	assert.Equal(t, err, nil)
	slog.Info("sync schema")
	err = kafkamq.SyncSchema()
	assert.Equal(t, err, nil)
	slog.Info("send message")
	msg := []byte("test message content" + time.Now().Format("2006-1-2 15:04:05"))
	err = kafkamq.SendMessage(msg)
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
			Topics:             topics,
			ConsumerGroup:      "my-event-group",
			NumOfPartition:     3,
			NumOfReplica:       2,
			AutoCommitSecond:   1,
			ProducerBufferSize: 1024,
			Initial:            sarama.OffsetNewest,
			Version:            sarama.V1_1_1_0,
			ClientID:           "microlibrary-kafka-client",
		},
		hosts:  []string{"localhost:9092"},
		logger: slog.Default(),
	}
	err := kafkamq.SendMessage(msg)
	assert.Equal(t, err, nil)
}
