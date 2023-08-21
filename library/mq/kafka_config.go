package mq

import (
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/IBM/sarama"
)

var configParseFuncs = make(map[string]configParseFunc)

var (
	// default = 9
	numOfPartition = 9
	// default = 3
	numOfReplica = 3
	// default 1s
	autoCommitSecond = 1
)

func init() {
	initconfigParsemap()
}

// KafkaConfig @Description:
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

type configParseFunc func(config *KafkaConfig, val string) error

func initconfigParsemap() {
	parseBasicConfig(configParseFuncs)
}

func NewdefaulKafkaConfig() *KafkaConfig {
	return &KafkaConfig{
		Topics:           []string{},
		ConsumerGroup:    "micorlib-group",
		NumOfPartition:   3,
		NumOfReplica:     2,
		AutoCommitSecond: 1,
		BufferSize:       1024,
		Initial:          sarama.OffsetNewest,
		Version:          sarama.V0_11_0_2,
		ClientID:         "microlib-client",
	}
}

func ParseConfig(param map[string]string) (*KafkaConfig, error) {
	var err error
	config := NewdefaulKafkaConfig()
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
	sconfig := sarama.NewConfig()
	sconfig.Version = c.Version
	sconfig.ClientID = c.ClientID
	// prodcuer
	sconfig.Producer.Return.Successes = true
	sconfig.Producer.RequiredAcks = sarama.WaitForAll
	sconfig.Producer.Flush.Messages = c.BufferSize
	// consumer
	sconfig.Consumer.Offsets.Initial = c.Initial
	sconfig.ChannelBufferSize = c.BufferSize
	sconfig.Consumer.Return.Errors = true
	sconfig.Consumer.Offsets.AutoCommit.Enable = true
	sconfig.Consumer.Offsets.AutoCommit.Interval = time.Duration(c.AutoCommitSecond * int(time.Second))
	return sconfig

}

func parseBasicConfig(m map[string]configParseFunc) {
	m["clientid"] = func(config *KafkaConfig, val string) error {
		config.ClientID = val
		return nil
	}
	m["topics"] = func(config *KafkaConfig, val string) error {
		config.Topics = strings.Split(val, ",")
		return nil
	}
	m["consumergroup"] = func(config *KafkaConfig, val string) error {
		config.ConsumerGroup = val
		return nil
	}
	m["numpartition"] = func(config *KafkaConfig, val string) error {
		var err error
		config.NumOfPartition, err = strconv.Atoi(val)
		return err
	}
	m["numreplica"] = func(config *KafkaConfig, val string) error {
		var err error
		config.NumOfReplica, err = strconv.Atoi(val)
		return err
	}
	m["inital"] = func(config *KafkaConfig, val string) error {
		var err error
		if val == "newest" {
			config.Initial = sarama.OffsetNewest
		} else if val == "oldest" {
			config.Initial = sarama.OffsetOldest
		} else {
			err = errors.New("inital must be newest or oldest")
		}
		return err
	}
	m["version"] = func(config *KafkaConfig, val string) error {
		var err error
		config.Version, err = sarama.ParseKafkaVersion(val)
		return err
	}
	m["buffersize"] = func(config *KafkaConfig, val string) error {
		var err error
		config.BufferSize, err = strconv.Atoi(val)
		return err
	}

}
