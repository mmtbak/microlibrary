package rdb

import (
	"fmt"
	"reflect"
	"time"

	"gorm.io/gorm/schema"
)

type DataBaseOption struct {
	Cluster string
	DBName  string
}

type ClickhouseTable interface {
	ClickhouseTableOption(option DataBaseOption) TableOption
}
type MySQLTable interface {
	MySQLTableOption(option DataBaseOption) TableOption
}

type TableOption struct {
	TableOptions   string
	ClusterOptions string
}

// DefaultClickHouseTable  默认clickhouse表
type DefaultClickHouseTable struct {
	// 采集时间
	Time time.Time `json:"time" gorm:"comment:时序时间"`
}

// TableOptionsClickHouse  gorm
func (table DefaultClickHouseTable) ClickhouseTableOption(dbop DataBaseOption) TableOption {
	top := TableOption{
		TableOptions: "ENGINE=MergeTree ORDER BY time PARTITION BY toYYYYMMDD(time)",
	}
	if dbop.Cluster != "" {
		top.ClusterOptions = fmt.Sprintf("on cluster %s", dbop.Cluster)
	}
	return top
}

// DefaultClickHouseDistributedTable
type DefaultClickHouseDistributedTable struct {
	// 采集时间
	Time time.Time `json:"time"  gorm:"comment:时序时间"`
}

// TableOptionsClickHouse 配置Clickhouse的创建options
func (table DefaultClickHouseDistributedTable) ClickhouseTableOption(dbop DataBaseOption) TableOption {
	item := new(DefaultClickHouseTable)
	table_name := schema.NamingStrategy{}.TableName(reflect.TypeOf(item).Elem().Name())
	top := TableOption{
		TableOptions: fmt.Sprintf("ENGINE=Distributed(%s, %s, %s, rand()", dbop.Cluster, dbop.DBName, table_name),
	}
	if dbop.Cluster != "" {
		top.ClusterOptions = fmt.Sprintf("on cluster %s", dbop.Cluster)
	}
	return top
}

type DefaultMySQLTable struct {
}

// MySQLTableOptions  gorm mysql option
func (table DefaultMySQLTable) MySQLTableOption(dbname string) TableOption {

	return TableOption{
		TableOptions: "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci",
	}
}
