package mq

import (
	"context"
	"time"
)

type SendMsgOption struct {
	Sendtime time.Time
	Key      string
}

func NewSendMsgOption() *SendMsgOption {
	return &SendMsgOption{
		Sendtime: time.Now(),
		Key:      "",
	}
}

func (opt *SendMsgOption) WithSendtime(t time.Time) *SendMsgOption {
	opt.Sendtime = t
	return opt
}

func (opt *SendMsgOption) WithKey(key string) *SendMsgOption {
	opt.Key = key
	return opt
}

func (opt *SendMsgOption) Build() SendMsgOption {
	return *opt
}

func MergeSendMsgOptions(opts []SendMsgOption) SendMsgOption {
	defaultopt := NewSendMsgOption()
	if len(opts) == 0 {
		return *defaultopt
	}
	return opts[0]
}

type ConsumeMsgOption struct {
	Poolsize int
	Ctx      context.Context
}

func NewConsumeMsgOption() *ConsumeMsgOption {
	return &ConsumeMsgOption{
		Poolsize: 10,
		Ctx:      context.Background(),
	}
}

func (opt *ConsumeMsgOption) WihtContext(ctx context.Context) *ConsumeMsgOption {
	opt.Ctx = ctx
	return opt
}

func (opt *ConsumeMsgOption) WihtPoolsize(size int) *ConsumeMsgOption {
	opt.Poolsize = size
	return opt
}
func (opt *ConsumeMsgOption) Build() ConsumeMsgOption {
	return *opt
}

func MergeConsumeMsgOptions(opts []ConsumeMsgOption) ConsumeMsgOption {
	defaultopt := NewConsumeMsgOption()
	if len(opts) == 0 {
		return *defaultopt
	}
	return opts[0]
}
