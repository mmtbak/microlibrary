package config

import (
	"testing"

	"gopkg.in/go-playground/assert.v1"
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
	assert.Equal(t, err, nil)
	conf := config{}
	err = fieloader.Load(&conf)
	assert.Equal(t, err, nil)
	excepted := config{
		Resource: struct {
			Database string
			Kafka    string
		}{
			Database: "mysql://user:password@not$valid@host/dbname?tblsprefix=fs_",
			Kafka:    "kafka://username:pasword@tcp(ip1:9093,ip2:9093,ip3:9093)/?topic=vsulblog",
		},
		Application: struct {
			Gateway struct {
				Concurrency int
				Filesize    int
			}
		}{
			Gateway: struct {
				Concurrency int
				Filesize    int
			}{
				Concurrency: 10,
				Filesize:    10,
			},
		},
	}
	assert.Equal(t, conf, excepted)
}
