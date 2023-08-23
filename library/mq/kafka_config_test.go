package mq

import (
	"fmt"
	"testing"

	"github.com/IBM/sarama"
)

func TestKafkaConfig(t *testing.T) {

	data := map[string]string{
		"inital":        "oldest",
		"version":       "2.8.1",
		"topics":        "my-event",
		"consumergroup": "mygroup",
		"myerrorconfig": "myerrvalue",
		// "version": "1.1.1",
	}
	cfg, err := ParseKafkaConfig(data)
	if err != nil {
		t.Error(err)
		return
	}
	fmt.Println(cfg)
	if cfg.Version != sarama.V2_8_1_0 {
		t.Error("version not match")
		return
	}

}
