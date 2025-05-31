package mq

import (
	"testing"
	"time"

	"github.com/IBM/sarama"
	"gopkg.in/go-playground/assert.v1"
)

func TestParseKafkaConfig(t *testing.T) {
	data := map[string]string{
		"initial":       "oldest",
		"version":       "2.8.1",
		"topics":        "my-event",
		"consumergroup": "mygroup",
		"buffersize":    "1024",
		"numpartition":  "3",
		"numreplica":    "2",
	}
	cfg, err := ParseKafkaConfig(data)
	assert.Equal(t, err, nil)
	exceptconfig := &KafkaConfig{
		NumOfPartition:     3,
		NumOfReplica:       2,
		AutoCommitSecond:   1,
		ProducerBufferSize: 1024,
		ProducerFrequency:  500 * time.Millisecond,
		Initial:            sarama.OffsetOldest,
		Version:            sarama.V2_8_1_0,
		Topics:             []string{"my-event"},
		ConsumerGroup:      "mygroup",
		ClientID:           "microlibrary-kafka-client",
	}
	assert.Equal(t, cfg, exceptconfig)
}
