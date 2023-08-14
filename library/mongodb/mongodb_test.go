package gorm

import (
	"fmt"
	"testing"

	"github.com/mmtbak/microlibrary/library/config"
)

func TestMongoClient(t *testing.T) {

	conf := config.AccessPoint{
		Source: "mongodb://mguser:mgpassword@9.135.250.244:27017,9.135.250.136:27017,9.135.251.28:27017/nimble_product?authSource=admin",
	}
	client, err := NewMongoClient(conf)
	if err != nil {
		fmt.Println(err)
		return
	}
	sv := client.Conn().ServerVersion()
	fmt.Println(sv)

	// err = client.Conn().Ping(int64(20 * time.Second))
	err = client.Conn().Ping(10)
	if err != nil {
		fmt.Println(err)
		return
	}

}
