package mq

import (
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/IBM/sarama"
)

type configParseFunc func(config *KafkaConfig, val string) error

var configParseFuncs = map[string]configParseFunc{
	"clientid": func(config *KafkaConfig, val string) error {
		config.ClientID = val
		return nil
	},
	"topics": func(config *KafkaConfig, val string) error {
		config.Topics = strings.Split(val, ",")
		return nil
	},
	"consumergroup": func(config *KafkaConfig, val string) error {
		config.ConsumerGroup = val
		return nil
	},
	"numpartition": func(config *KafkaConfig, val string) error {
		var err error
		config.NumOfPartition, err = strconv.Atoi(val)
		return err
	},
	"numreplica": func(config *KafkaConfig, val string) error {
		var err error
		config.NumOfReplica, err = strconv.Atoi(val)
		return err
	},
	"initial": func(config *KafkaConfig, val string) error {
		var err error
		if val == "newest" {
			config.Initial = sarama.OffsetNewest
		} else if val == "oldest" {
			config.Initial = sarama.OffsetOldest
		} else {
			err = errors.New("initial must be newest or oldest")
		}
		return err
	},
	"version": func(config *KafkaConfig, val string) error {
		var err error
		config.Version, err = sarama.ParseKafkaVersion(val)
		return err
	},
	"buffersize": func(config *KafkaConfig, val string) error {
		var err error
		config.BufferSize, err = strconv.Atoi(val)
		return err
	},
}

// KafkaConfig @Description:.
type KafkaConfig struct {
	Topics           []string // @Description: 接受消息的topic
	ConsumerGroup    string   // @Description: 消费者组名称
	NumOfPartition   int
	NumOfReplica     int
	AutoCommitSecond int
	BufferSize       int
	Initial          int64 // 最新偏移消息
	Version          sarama.KafkaVersion
	ClientID         string
}

func NewDefaultKafkaConfig() *KafkaConfig {
	return &KafkaConfig{
		Topics:           []string{},
		ConsumerGroup:    "microlibrary-kafka-group",
		NumOfPartition:   3,
		NumOfReplica:     2,
		AutoCommitSecond: 1,
		BufferSize:       1024,
		Initial:          sarama.OffsetNewest,
		Version:          sarama.V1_1_1_0,
		ClientID:         "microlibrary-kafka-client",
	}
}

func ParseKafkaConfig(param map[string]string) (*KafkaConfig, error) {
	var err error
	config := NewDefaultKafkaConfig()
	for name, val := range param {
		if parseFunc, ok := configParseFuncs[name]; ok {
			err = parseFunc(config, val)
			if err != nil {
				return nil, err
			}
		}
	}

	return config, nil
}

func (c *KafkaConfig) GenConfig() *sarama.Config {
	newconfig := sarama.NewConfig()
	newconfig.Version = c.Version
	newconfig.ClientID = c.ClientID
	// producer
	newconfig.Producer.Return.Successes = true
	newconfig.Producer.RequiredAcks = sarama.WaitForAll
	newconfig.Producer.Flush.Messages = c.BufferSize
	// consumer
	newconfig.Consumer.Offsets.Initial = c.Initial
	newconfig.ChannelBufferSize = c.BufferSize
	newconfig.Consumer.Return.Errors = true
	newconfig.Consumer.Offsets.AutoCommit.Enable = true
	newconfig.Consumer.Offsets.AutoCommit.Interval = time.Duration(c.AutoCommitSecond * int(time.Second))
	return newconfig
}
