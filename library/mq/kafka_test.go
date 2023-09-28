package mq

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/mmtbak/microlibrary/library/config"
)

const messageText = "message test "

func TestKafkaMessageQueue(t *testing.T) {
	var kafkaconf = config.AccessPoint{
		Source: "kafka://127.0.0.1:9092/?topics=my-event&consumergroup=my-event-group&numpartition=3&numreplica=3&inital=oldest",
	}
	kafkamq, err := NewKafkaMessageQueue(kafkaconf)
	if err != nil {
		t.Error(err)
		return
	}
	sendMsg := []byte(messageText + time.Now().Format("2006-1-2 15:04:05"))
	sopt := NewSendMsgOption().WithSendtime(time.Now()).Build()
	if err = kafkamq.SendMessage(sendMsg, sopt); err != nil {
		t.Error(err)
	}
	fmt.Println("send --->", string(sendMsg))
	ok := false
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	copt := NewConsumeMsgOption().WihtContext(ctx).Build()
	if err := kafkamq.ConsumeMessage(func(msg IMessage) {
		message := string(msg.Body())
		fmt.Println("recv --->", message)
		if message == string(sendMsg) {
			ok = true
			cancel()
		}
		_ = msg.Ack()
	}, copt); err != nil {
		t.Error(err)
	}
	if !ok {
		t.Fail()
	}
}

func TestKafkaSendMessage(t *testing.T) {
	var kafkaconf = config.AccessPoint{
		Source: "kafka://127.0.0.1:9092/?topic=my-event-topic",
		Options: map[string]interface{}{
			"consumergroup": "my-event-grp",
			"numpartition":  3,
			"numreplica":    3,
		},
	}
	kafkamq, err := NewKafkaMessageQueue(kafkaconf)
	if err != nil {
		t.Error(err)
		return
	}
	msg := []byte(messageText + time.Now().Format("2006-1-2 15:04:05"))
	if err = kafkamq.SendMessage(msg); err != nil {
		t.Error(err)
	}
	fmt.Println("send --->", string(msg))
}

func TestKafkaOffsetOldest(t *testing.T) {
	var kafkaconf = config.AccessPoint{
		Source: "kafka://127.0.0.1:9092/?topic=my-event-topic",
		Options: map[string]interface{}{
			"consumergroup": "my-event-grp",
			"numpartition":  3,
			"numreplica":    3,
			"buffersize":    100,
			"offsetnewest":  false,
		},
	}
	kafkamq, err := NewKafkaMessageQueue(kafkaconf)
	if err != nil {
		t.Error(err)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	copt := NewConsumeMsgOption().WihtContext(ctx).Build()
	go func() {
		if err := kafkamq.ConsumeMessage(func(msg IMessage) {
			fmt.Println("recv --->", string(msg.Body()))
			_ = msg.Ack()
		}, copt); err != nil {
			t.Error(err)
		}
	}()
	<-ctx.Done()
	cancel()
}

func TestKafkaConsumeMessage(t *testing.T) {
	var kafkaconf = config.AccessPoint{
		Source: "kafka://user:password@127.0.0.1:9092/?topics=my-subscription-topic",
		Options: map[string]interface{}{
			"consumergroup": "mygroup",
			"numpartition":  3,
			"numreplica":    3,
			"buffersize":    100,
			"offsetnewest":  true,
		},
	}
	kafkamq, err := NewKafkaMessageQueue(kafkaconf)
	if err != nil {
		t.Error(err)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	copt := NewConsumeMsgOption().WihtContext(ctx).WihtPoolsize(1).Build()
	go func() {
		if err := kafkamq.ConsumeMessage(func(msg IMessage) {
			fmt.Println("recv --->", string(msg.Body()))
			_ = msg.Ack()
		}, copt); err != nil {
			t.Error(err)
		}
	}()
	<-ctx.Done()
	cancel()
}

func TestKafkaReceiveMessageNoAck(t *testing.T) {
	var kafkaconf = config.AccessPoint{
		Source: "kafka://127.0.0.1:9092/?topic=my-event",
		Options: map[string]interface{}{
			"consumergroup": "my-event-grp",
			"numpartition":  3,
			"numreplica":    3,
			"buffersize":    100,
			"offsetnewest":  false,
		},
	}
	kafkamq, err := NewKafkaMessageQueue(kafkaconf)
	if err != nil {
		t.Error(err)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	copt := NewConsumeMsgOption().WihtContext(ctx).WihtPoolsize(1).Build()
	go func() {
		if err := kafkamq.ConsumeMessage(func(msg IMessage) {
			fmt.Println("recv --->", string(msg.Body()))
		}, copt); err != nil {
			t.Error(err)
		}
	}()
	<-ctx.Done()
	cancel()
}

func TestKafkaConsumeMessageOption(t *testing.T) {
	var kafkaconf = config.AccessPoint{
		Source: "kafka://127.0.0.1:9092/?topics=my-subscription-topic&consumegroup=mygroup&version=1.1.1",
	}
	kafkamq, err := NewKafkaMessageQueue(kafkaconf)
	if err != nil {
		t.Error(err)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	// ctx, cancel := context.WithCancel(context.Background())
	copt := NewConsumeMsgOption().WihtContext(ctx).WihtPoolsize(10).Build()

	err = kafkamq.ConsumeMessage(func(msg IMessage) {
		fmt.Println("recivev message ", msg.ID())
		fmt.Println("recv --->", string(msg.Body()))
		_ = msg.Ack()
	}, copt)
	if err != nil {
		fmt.Println(err)
		t.Error(err)
	}

	<-ctx.Done()
	cancel()
}
