package gorm

import (
	"fmt"
	"testing"

	"github.com/mmtbak/microlibrary/library/config"
)

func TestDBClient(t *testing.T) {

	var ac = config.AccessPoint{
		Source: "mysql://root:password@tcp(127.0.0.1:3306)/mydb?charset=utf8&parseTime=true&loc=Local",
		Options: map[string]interface{}{
			"maxopenconn": 1000,
			"maxidleconn": 1000,
			"sqllog":      true,
		},
	}
	client, err := NewDBClient(ac)
	if err != nil {
		fmt.Println(err)
		return
	}

	var tables []string
	tx := client.DB().Raw("show tables ").Scan(&tables)
	if tx.Error != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(tables)
}

func TestClickHouseDBClient(t *testing.T) {

	var ac = config.AccessPoint{
		Source: "clickhouse://root:password@127.0.0.1:9000/mydb?read_timeout=10s",
		Options: map[string]interface{}{
			"maxopenconn": 1000,
			"maxidleconn": 1000,
			"sqllog":      true,
		},
	}

	client, err := NewDBClient(ac)
	if err != nil {
		fmt.Println(err)
		return
	}
	var tables []string
	tx := client.DB().Raw("show tables ").Scan(&tables)
	if tx.Error != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(tables)
}
