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

func (opt *ConsumeMsgOption) WithContext(ctx context.Context) *ConsumeMsgOption {
	opt.Ctx = ctx
	return opt
}

func (opt *ConsumeMsgOption) WithPoolsize(size int) *ConsumeMsgOption {
	opt.Poolsize = size
	return opt
}

func MergeConsumeMsgOptions(opts []ConsumeMsgOption) ConsumeMsgOption {
	defaultopt := NewConsumeMsgOption()
	if len(opts) == 0 {
		return *defaultopt
	}
	return opts[0]
}
