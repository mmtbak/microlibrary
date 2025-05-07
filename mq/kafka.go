package mq

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/IBM/sarama"
	"github.com/mmtbak/microlibrary/config"
	"github.com/pkg/errors"
)

// KafkaMessageQueue  kafka实现的队列
type KafkaMessageQueue struct {
	access   config.AccessPoint
	config   *KafkaConfig
	dsndata  config.DSN
	hosts    []string
	topics   []string
	producer sarama.SyncProducer
	consumer sarama.ConsumerGroup
	logger   *slog.Logger
}

// NewKafkaMessageQueue new message queue
func NewKafkaMessageQueue(conf config.AccessPoint) (*KafkaMessageQueue, error) {
	var err error
	var config *KafkaConfig

	dsndata, err := conf.Decode(nil)
	if err != nil {
		return nil, err
	}
	// 转换成小写
	hoststr := dsndata.Hostport
	hosts := strings.Split(hoststr, ",")
	// params 的解析

	if dsndata.Params != nil {
		config, err = ParseKafkaConfig(dsndata.Params)
		if err != nil {
			return nil, err
		}
	}

	topics := config.Topics
	if len(topics) == 0 {
		return nil, errors.New("topics is empty")
	}

	// producer 发送时创建
	// consumer group 消费时创建
	kafkamq := &KafkaMessageQueue{
		access:   conf,
		config:   config,
		dsndata:  dsndata,
		hosts:    hosts,
		topics:   topics,
		producer: nil,
		consumer: nil,
		logger:   slog.Default(),
	}

	// Test Kafka connection
	cfg := kafkamq.GenConfig()
	clusteradmin, err := sarama.NewClusterAdmin(hosts, cfg)
	if err != nil {
		return nil, err
	}
	defer clusteradmin.Close()

	return kafkamq, nil
}

// CreateTopics create topics
func (mq *KafkaMessageQueue) CreateTopics() error {
	var err error
	for _, t := range mq.topics {
		err = mq.CreateTopic(t)
		if err != nil {
			return err
		}
	}
	return nil
}

// SetLogger add set logger method for mq interface
func (mq *KafkaMessageQueue) SetLogger(l *slog.Logger) {
	mq.logger = l
}

// CreateTopic  create topic if not exist
// param topic name
func (mq *KafkaMessageQueue) CreateTopic(topic string) error {
	// Set broker configuration
	var err error
	cfg := mq.GenConfig()
	clusteradmin, err := sarama.NewClusterAdmin(mq.hosts, cfg)
	if err != nil {
		return err
	}
	defer func() {
		_ = clusteradmin.Close()
	}()

	topicmap, err := clusteradmin.ListTopics()
	if err != nil {
		return err
	}
	_, ok := topicmap[topic]
	if ok {
		return nil
	}

	topicDetail := &sarama.TopicDetail{}
	topicDetail.NumPartitions = int32(mq.config.NumOfPartition)
	topicDetail.ReplicationFactor = int16(mq.config.NumOfReplica)
	topicDetail.ConfigEntries = make(map[string]*string)

	err = clusteradmin.CreateTopic(topic, topicDetail, false)
	if err != nil {
		return err
	}
	return nil
}

// SyncSchema implements create topics
func (mq *KafkaMessageQueue) SyncSchema() error {
	return mq.CreateTopics()
}

func (mq *KafkaMessageQueue) newProducer() (sarama.SyncProducer, error) {
	var err error
	if mq.producer == nil {
		producerconfig := mq.GenConfig()
		mq.producer, err = sarama.NewSyncProducer(mq.hosts, producerconfig)
		if err != nil {
			return nil, errors.Wrap(err, "new producer failed")
		}
	}

	return mq.producer, nil
}

func (mq *KafkaMessageQueue) newConsumer() (sarama.ConsumerGroup, error) {
	var err error
	var consumer sarama.ConsumerGroup
	cfg := mq.GenConfig()

	consumer, err = sarama.NewConsumerGroup(mq.hosts, mq.config.ConsumerGroup, cfg)
	if err != nil {
		return nil, err
	}

	return consumer, err
}

func (mq *KafkaMessageQueue) GenConfig() *sarama.Config {

	config := mq.config.GenConfig()
	if mq.dsndata.User != "" || mq.dsndata.Password != "" {
		config.Net.SASL.Enable = true
		config.Net.SASL.User = mq.dsndata.User
		config.Net.SASL.Password = mq.dsndata.Password
		config.Net.SASL.Mechanism = sarama.SASLTypePlaintext

	}
	return config
}

// SendMessage implements
func (mq *KafkaMessageQueue) SendMessage(msg []byte, opts ...*SendMsgOption) error {
	opt := NewSendMsgOption()
	if len(opts) > 0 {
		opt = opts[0]
	}
	producer, err := mq.newProducer()
	if err != nil {
		return err
	}
	producerMsg := &sarama.ProducerMessage{
		Value: sarama.ByteEncoder(msg),
	}
	if !opt.Sendtime.IsZero() {
		producerMsg.Timestamp = opt.Sendtime
	}
	if opt.Key != "" {
		producerMsg.Key = sarama.StringEncoder(opt.Key)
	}
	for _, topic := range mq.topics {
		producerMsg.Topic = topic
		if _, _, err := producer.SendMessage(producerMsg); err != nil {
			return err
		}
	}
	return err
}

// ReceiveMessage receive message
func (mq *KafkaMessageQueue) ReceiveMessage() (<-chan Message, error) {
	consumer, err := mq.newConsumer()
	if err != nil {
		return nil, err
	}
	handler := &kafkaConsumerGroupHandler{
		queue: mq,
		msg:   make(chan Message),
	}
	go func() {
		for {
			err = consumer.Consume(context.Background(), mq.topics, handler)
			if err != nil {
				if errors.Is(err, sarama.ErrOutOfBrokers) {
					err = errors.Wrap(sarama.ErrOutOfBrokers, "conn disconnect")
					mq.logger.Error("kafka conn disconnect", "error", err)
				}
			}
		}
	}()
	return handler.msg, nil
}

// Close mq
func (mq *KafkaMessageQueue) Close() error {
	if mq.producer != nil {
		return mq.producer.Close()
	}
	if mq.consumer != nil {
		return mq.consumer.Close()
	}
	return nil
}

// KafkaMessage message
type KafkaMessage struct {
	session sarama.ConsumerGroupSession
	msg     *sarama.ConsumerMessage
	handler *kafkaConsumerGroupHandler
	Marked  bool
}

// Body msg context
func (msg *KafkaMessage) Body() []byte {
	return msg.msg.Value
}

// ID partition offset
func (msg *KafkaMessage) ID() string {
	return fmt.Sprintf("partition-%d,offset-%d", msg.msg.Partition, msg.msg.Offset)
}

// Ack reply ack
func (msg *KafkaMessage) Ack() error {
	msg.session.MarkMessage(msg.msg, "")
	return nil
}

// Nack no ack
func (msg *KafkaMessage) Nack() error {
	return nil
}

// kafkaConsumerGroupHandler consume interface
type kafkaConsumerGroupHandler struct {
	queue *KafkaMessageQueue
	msg   chan Message
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (h *kafkaConsumerGroupHandler) Setup(session sarama.ConsumerGroupSession) error {
	h.queue.logger.Info("kafka setup")
	if h.msg == nil {
		h.msg = make(chan Message)
	}
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (h *kafkaConsumerGroupHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	h.queue.logger.Info("kafka cleanup")
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (h *kafkaConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim) error {
	msgchan := claim.Messages()
	for {
		select {
		case message := <-msgchan:
			msg := &KafkaMessage{
				session: session,
				msg:     message,
				handler: h,
				Marked:  false,
			}
			h.msg <- msg
		// Should return when `session.Context()` is done.
		// If not, will raise `ErrRebalanceInProgress` or `read tcp <ip>:<port>: i/o timeout` when kafka rebalance. see:
		// https://github.com/Shopify/sarama/issues/1192
		case <-session.Context().Done():
			return nil
		}
	}
}
