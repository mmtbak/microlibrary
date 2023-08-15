package rdb

import (
	"fmt"
	"testing"
)

func TestTableOption(t *testing.T) {

	var err error
	fmt.Println(dbmysqlclient)
	tables := []interface{}{
		new(DefaultMySQLTable),
	}
	err = dbmysqlclient.SyncTables(tables)
	if err != nil {
		fmt.Println(err)
		return
	}
}
