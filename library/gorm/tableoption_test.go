package gorm

import (
	"fmt"
	"testing"

	"github.com/mmtbak/microlibrary/library/config"
)

func TestTableOption(t *testing.T) {

	ac := config.AccessPoint{
		Source: "clickhouse://user:password@127.0.0.1:9000/om2_test?cread_timeout=10s&write_timeout=20s",
		Options: map[string]interface{}{
			"MaxIdleConn": 1000,
			"MaxOpenConn": 1000,
			"SQLLog":      true,
		},
	}
	client, err := NewDBClient(ac)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(client)
	tables := []interface{}{
		new(DefaultClickHouseTable),
	}
	err = client.SyncTables(tables)
	if err != nil {
		fmt.Println(err)
		return
	}
}
