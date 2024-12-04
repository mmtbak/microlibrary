package mq

import (
	"testing"

	"github.com/IBM/sarama"
	"gopkg.in/go-playground/assert.v1"
)

func TestParseKafkaConfig(t *testing.T) {
	data := map[string]string{
		"initial":       "oldest",
		"version":       "2.8.1",
		"topics":        "my-event",
		"consumergroup": "mygroup",
		"myerrorconfig": "myerrvalue",
		// "version": "1.1.1",
	}
	cfg, err := ParseKafkaConfig(data)
	assert.Equal(t, err, nil)
	exceptconfig := &KafkaConfig{
		NumOfPartition:   3,
		NumOfReplica:     2,
		AutoCommitSecond: 1,
		BufferSize:       1024,
		Initial:          sarama.OffsetOldest,
		Version:          sarama.V2_8_1_0,
		Topics:           []string{"my-event"},
		ConsumerGroup:    "mygroup",
		ClientID:         "microlib-client",
	}
	assert.Equal(t, cfg, exceptconfig)
}
