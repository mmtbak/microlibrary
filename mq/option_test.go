package mq

import (
	"fmt"
	"testing"
	"time"
)

func TestSendMsgOption(t *testing.T) {

	opt := NewSendMsgOption().WithKey("abc").WithSendtime(time.Now())

	fmt.Println(opt)
}
