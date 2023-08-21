package mq

import (
	"fmt"
	"strings"
	"sync"

	"github.com/IBM/sarama"
	"github.com/mmtbak/microlibrary/library/config"
	"github.com/panjf2000/ants/v2"
	"github.com/pkg/errors"
	"golang.org/x/exp/slog"
)

// KafkaMessageQueue  kafka实现的队列
type KafkaMessageQueue struct {
	access       config.AccessPoint
	hosts        []string
	topics       []string
	producer     sarama.SyncProducer
	config       *KafkaConfig
	producerOnce sync.Once
	user         string
	password     string
}

// NewKafkaMessageQueue new message queue
func NewKafkaMessageQueue(conf config.AccessPoint) (IMessageQueue, error) {
	var err error
	var config *KafkaConfig

	dsndata, err := conf.Decode(nil, nil)
	if err != nil {
		return nil, err
	}
	// 转换成小写
	hoststr := dsndata.Hostport
	hosts := strings.Split(hoststr, ",")
	// params 的解析

	if dsndata.Params == nil {
		config, err = ParseConfig(dsndata.Params)
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
	kafkamq := KafkaMessageQueue{
		access:   conf,
		config:   config,
		hosts:    hosts,
		topics:   topics,
		producer: nil,
		user:     dsndata.User,
		password: dsndata.Password,
	}
	return &kafkamq, nil
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

// CreateTopic  create topic if not exist
// param topic name
func (mq *KafkaMessageQueue) CreateTopic(topic string) error {
	// Set broker configuration
	var err error
	cfg := sarama.NewConfig()
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
	topicDetail.ReplicationFactor = int16(mq.config.NumOfPartition)
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

func (mq *KafkaMessageQueue) getProducer() (sarama.SyncProducer, error) {
	var err error
	mq.producerOnce.Do(func() {
		prodconfig := mq.config.GenConfig()
		mq.producer, err = sarama.NewSyncProducer(mq.hosts, prodconfig)
	})
	if err != nil {
		return nil, errors.Wrap(err, "new producer failed")
	}
	return mq.producer, nil
}

func (mq *KafkaMessageQueue) newConsumer() (sarama.ConsumerGroup, error) {
	var err error
	var consumer sarama.ConsumerGroup
	consumerconfig := mq.config.GenConfig()
	if mq.user != "" && mq.password != "" {
		consumerconfig.Net.SASL.Enable = true
		consumerconfig.Net.SASL.User = mq.user
		consumerconfig.Net.SASL.Password = mq.password
		consumerconfig.Net.SASL.Mechanism = sarama.SASLTypePlaintext
	}
	consumer, err = sarama.NewConsumerGroup(mq.hosts, mq.config.ConsumerGroup, consumerconfig)
	if err != nil {
		slog.Error("err:%s", err)
		return nil, errors.Wrap(err, "new consumer group failed")
	}
	return consumer, nil
}

// SendMessage implements
func (mq *KafkaMessageQueue) SendMessage(msg []byte, opts ...SendMsgOption) error {
	opt := MergeSendMsgOptions(opts)
	producer, err := mq.getProducer()
	if err != nil {
		return err
	}
	for _, topic := range mq.topics {
		if _, _, err := producer.SendMessage(&sarama.ProducerMessage{
			Topic:     topic,
			Value:     sarama.ByteEncoder(msg),
			Timestamp: opt.Sendtime,
			// send key
			Key: sarama.StringEncoder(opt.Key),
		}); err != nil {
			return err
		}
	}
	return err
}

// ConsumeMessage start consume block , support reconnect, still waiting for user to cancel
func (mq *KafkaMessageQueue) ConsumeMessage(cb ConsumeMessageFunc, opts ...ConsumeMsgOption) error {
	opt := MergeConsumeMsgOptions(opts)
	if cb == nil {
		return fmt.Errorf("callback must not be nil")
	}
	consumer, err := mq.newConsumer()
	if err != nil {
		return err
	}
	// pool func
	var pf = func(i interface{}) {
		msg, ok := i.(*KafkaMessage)
		if !ok {
			return
		}
		cb(msg)
	}
	pool, err := ants.NewPoolWithFunc(opt.Poolsize, pf, ants.WithNonblocking(false))
	if err != nil {
		return err
	}
	defer pool.Release()
	// consume
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		handler := &kafkaConsumerGroupHandler{
			pool: pool,
		}
		for {
			// still waiting for user to cancel
			if err := consumer.Consume(opt.Ctx, mq.topics, handler); err != nil {
				// 当setup失败的时候，error会返回到这里
				if errors.Is(err, sarama.ErrOutOfBrokers) {
					err = errors.Wrap(sarama.ErrOutOfBrokers, "conn disconnect")
				}
				slog.Error("kafka error", err)
			}
			// check if context was cancelled, signaling that the consumer should stop
			if opt.Ctx.Err() != nil {
				err = errors.Errorf("context was cancelled")
				return
			}
		}
	}()
	wg.Wait()
	if err = consumer.Close(); err != nil {
		return err
	}
	return err
}

// Close mq
func (mq *KafkaMessageQueue) Close() error {
	return nil
}

// KafkaMessage message
type KafkaMessage struct {
	session sarama.ConsumerGroupSession
	msg     *sarama.ConsumerMessage
	body    []byte
}

// Body msg context
func (msg *KafkaMessage) Body() []byte {
	return msg.body
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
	pool *ants.PoolWithFunc
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (h *kafkaConsumerGroupHandler) Setup(session sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	fmt.Println("kafka setup", session.Claims())
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (h *kafkaConsumerGroupHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	fmt.Println("kafka cleanup", session.Claims())
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (h *kafkaConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim) error {
	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/main/consumer_group.go#L27-L29
	for {
		select {
		case message := <-claim.Messages():
			//log.Printf("Message claimed: value = %s, timestamp = %v, topic = %s",
			//	string(message.Value), message.Timestamp, message.Topic)
			msg := KafkaMessage{
				session: session,
				msg:     message,
				body:    message.Value,
			}
			if err := h.pool.Invoke(&msg); err != nil {
				fmt.Println(err)
			}
		// Should return when `session.Context()` is done.
		// If not, will raise `ErrRebalanceInProgress` or `read tcp <ip>:<port>: i/o timeout` when kafka rebalance. see:
		// https://github.com/Shopify/sarama/issues/1192
		case <-session.Context().Done():
			return nil
		}
	}
}
