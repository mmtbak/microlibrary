package rdb

import (
	"fmt"
	"testing"

	"github.com/mmtbak/microlibrary/library/config"
)

func TestMongoClient(t *testing.T) {

	conf := config.AccessPoint{
		Source: "mongodb://mguser:mgpassword@127.0.0.1:27017,127.0.0.2:27017,127.0.0.3:27017/my_product?authSource=admin",
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
