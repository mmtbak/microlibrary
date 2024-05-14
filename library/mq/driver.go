package mq

type MQDriver interface {
	// 订阅
	Subscribe(topic string, handler func(msg []byte)) error
	// 发布
	Publish(topic string, msg []byte) error
}
