package rdb

import (
	"reflect"
	"time"

	"gorm.io/gorm/schema"
)

type TableWithOption interface {
	TableOptions(dbname string) TableOption
}

type TableOption struct {
	TableOptions   string
	ClusterOptions string
}

// DefaultClickHouseTable  默认clickhouse表
type DefaultClickHouseTable struct {
	// 采集时间
	Time time.Time `json:"time"  gorm:"comment:时序时间"`
}

// TableOptionsClickHouse  gorm
func (table DefaultClickHouseTable) TableOptions(dbname string) TableOption {
	return TableOption{
		TableOptions:   "ENGINE=ReplicatedMergeTree ORDER BY time PARTITION BY toYYYYMMDD(time)",
		ClusterOptions: "on cluster default_cluster",
	}
}

// DefaultClickHouseDistributedTable
type DefaultClickHouseDistributedTable struct {
	// 采集时间
	Time time.Time `json:"time"  gorm:"comment:时序时间"`
}

// TableOptionsClickHouse 配置Clickhouse的创建options
func (table DefaultClickHouseDistributedTable) TableOptions(dbname string) TableOption {
	item := new(DefaultClickHouseTable)
	table_name := schema.NamingStrategy{}.TableName(reflect.TypeOf(item).Elem().Name())
	return TableOption{
		TableOptions:   "ENGINE=Distributed(default_cluster, " + dbname + ", " + table_name + ",rand())",
		ClusterOptions: "on cluster default_cluster",
	}
}
