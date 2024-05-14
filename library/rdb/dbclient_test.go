package rdb

import (
	"fmt"
	"testing"

	"github.com/mmtbak/microlibrary/library/config"
	"gorm.io/gorm"
)

var dbmysqlclient *DBClient

func TestDBClient(t *testing.T) {

	var tables []string
	var err error
	address := config.AccessPoint{
		Source: "mysql://root:password@tcp(127.0.0.1:3306)/my_dev?charset=utf8&parseTime=true&loc=Local",
		Options: map[string]interface{}{
			"MaxIdleConn": 100,
			"MaxOpenConn": 100,
			"SQLLevel":    "info",
		},
	}

	dbmysqlclient, err = NewDBClient(address, &gorm.Config{
		DryRun: true,
	})
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(dbmysqlclient)
	tx := dbmysqlclient.DB().Raw("show tables ").Scan(&tables)
	if tx.Error != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(tables)
}

func TestClickHouseDBClient(t *testing.T) {

	var ac = config.AccessPoint{
		// Source: "clickhouse://root:password@127.0.0.1:9000/mydb?read_timeout=10s",
		Source: "clickhouse://9.135.63.32:9000/testdb?dial_timeout=10s&read_timeout=20s",
		Options: map[string]interface{}{
			"maxopenconn": 1000,
			"maxidleconn": 1000,
			"sqllevel":    "info",
			"cluster":     "",
		},
	}

	client, err := NewDBClient(ac, &gorm.Config{
		// DryRun: true,
	})
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

	// newtables := []interface{}{new(DefaultClickHouseTable), new(DefaultClickHouseTable), new(DefaultClickHouseDistributedTable)}
	newtables := []interface{}{new(DefaultClickHouseTable), new(DefaultClickHouseDistributedTable)}
	err = client.SyncTables(newtables)
	fmt.Println(err)

}
