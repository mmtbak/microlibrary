package mq

import (
	"testing"
	"time"

	"gopkg.in/go-playground/assert.v1"
)

func TestSendMsgOption(t *testing.T) {
	sendtime := time.Now()
	opt := NewSendMsgOption().WithKey("abc").WithSendtime(sendtime)
	exceptoption := &SendMsgOption{
		Key:      "abc",
		Sendtime: sendtime,
	}
	assert.Equal(t, opt, exceptoption)
}
