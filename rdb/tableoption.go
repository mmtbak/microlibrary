package rdb

import (
	"fmt"
	"reflect"
	"time"

	"gorm.io/gorm/schema"
)

type DataBaseOption struct {
	Cluster      string
	DatabaseName string
}

// TableOption table option.
type TableOption struct {
	TableOptions string
	// TableClusterOptions for clickhouse distributed table.
	TableClusterOptions string
}

// MySQLTable mysql table interface.
type MySQLTable interface {
	MySQLTableOption(option DataBaseOption) TableOption
}

// DefaultMySQLTable default mysql table field.
type DefaultMySQLTable struct{}

// MySQLTableOption  gorm mysql option.
func (table DefaultMySQLTable) MySQLTableOption(_ string) TableOption {
	return TableOption{
		TableOptions: "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci",
	}
}

// ClickhouseTable clickhouse table interface.
type ClickhouseTable interface {
	ClickhouseTableOption(option DataBaseOption) TableOption
}

// DefaultClickHouseTable  默认clickhouse表.
type DefaultClickHouseTable struct {
	// 采集时间
	Time time.Time `json:"time" gorm:"comment:时序时间"`
}

// ClickhouseTableOption  gorm.
func (table DefaultClickHouseTable) ClickhouseTableOption(dbop DataBaseOption) TableOption {
	top := TableOption{
		TableOptions: "ENGINE=MergeTree ORDER BY time PARTITION BY toYYYYMMDD(time)",
	}
	if dbop.Cluster != "" {
		top.TableClusterOptions = fmt.Sprintf("on cluster %s", dbop.Cluster)
	}
	return top
}

// DefaultClickHouseDistributedTable default clickhouse table field.
type DefaultClickHouseDistributedTable struct {
	// 采集时间
	Time time.Time `json:"time"  gorm:"comment:时序时间"`
}

// ClickhouseTableOption 配置Clickhouse的创建options.
func (table DefaultClickHouseDistributedTable) ClickhouseTableOption(dbop DataBaseOption) TableOption {
	item := new(DefaultClickHouseTable)
	table_name := schema.NamingStrategy{}.TableName(reflect.TypeOf(item).Elem().Name())
	top := TableOption{
		TableOptions: fmt.Sprintf("ENGINE=Distributed(%s, %s, %s, rand()", dbop.Cluster, dbop.DatabaseName, table_name),
	}
	if dbop.Cluster != "" {
		top.TableClusterOptions = fmt.Sprintf("on cluster %s", dbop.Cluster)
	}
	return top
}
