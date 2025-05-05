package config

import (
	"log"
	"testing"
)

type config struct {
	Resource struct {
		Database string
		Kafka    string
	}
	Application struct {
		Gateway struct {
			Concurrency int
			Filesize    int
		}
	}
}

func TestFileConfig(t *testing.T) {

	filedsn := "file://example/config.yaml?codec=yaml"
	fieloader, err := NewFileLoader(filedsn)
	if err != nil {
		log.Println(err)
		return
	}
	conf := config{}
	err = fieloader.Load(&conf)
	if err != nil {
		log.Println(err)
		return
	}
	log.Println(conf)

}
