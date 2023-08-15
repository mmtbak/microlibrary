package rdb

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/mmtbak/microlibrary/library/config"

	_ "github.com/go-sql-driver/mysql" // mysql driver
	"gorm.io/driver/clickhouse"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// DBClient Gorm的数据库连接
type DBClient struct {
	schmea   string
	database string
	conn     *gorm.DB
	conf     config.AccessPoint
	options  DBClientOptions
}

// DBClientOptions dbclient 配置选项
type DBClientOptions struct {
	MaxOpenConn int
	MaxIdleConn int
	MaxIdleTime string
	SQLLevel    string
}

// LogLevelMap log level for config string
var LogLevelMap = map[string]logger.LogLevel{
	"info":   logger.Info,
	"error":  logger.Error,
	"silent": logger.Silent,
}

// NewDBClient Create DBEngine instance
func NewDBClient(conf config.AccessPoint, opts ...gorm.Option) (*DBClient, error) {

	var conn *gorm.DB
	var err error

	op := DBClientOptions{
		MaxOpenConn: 100,
		MaxIdleConn: 100,
	}

	dsn, err := conf.Decode(&op)
	if err != nil {
		return nil, err
	}

	switch dsn.Scheme {
	case "mysql":
		conn, err = gorm.Open(mysql.Open(dsn.Source), opts...)
	case "clickhouse":
		conn, err = gorm.Open(clickhouse.Open(conf.Source), opts...)
	default:
		return nil, fmt.Errorf("no database type : [%s]", dsn.Scheme)
	}
	if err != nil {
		return nil, err
	}
	dbname := dsn.Path

	client := &DBClient{
		schmea:   dsn.Scheme,
		database: dbname,
		conn:     conn,
		conf:     conf,
		options:  op,
	}
	err = client.ConfigOptions(op)
	if err != nil {
		return nil, err
	}
	return client, nil
}

// Session Session
func (client *DBClient) Session() *gorm.DB {
	return client.conn.Session(&gorm.Session{})
}

// NewSession Session
func (client *DBClient) NewSession() *gorm.DB {
	return client.conn.Begin()
}

// DB DB
func (client *DBClient) DB() *gorm.DB {
	return client.conn
}

// SyncTables sync tables defined in  table object
func (client *DBClient) SyncTables(tables []interface{}) error {

	var err error
	for _, table := range tables {
		tx, maker := NewSessionMaker(nil, client)
		defer maker.Close(&err)
		var opt TableOption

		// 如果是clickhouse 表
		if cktable, ok := table.(ClickhouseTable); ok {
			opt = cktable.ClickhouseTableOptions(client.database)
			if opt.TableOptions != "" {
				tx = tx.Set("gorm:table_options", opt.TableOptions)
			}
			if opt.ClusterOptions != "" {
				tx = tx.Set("gorm:table_cluster_options", opt.ClusterOptions)
			}
		} else if mytable, ok := table.(MySQLTable); ok {

			opt = mytable.MySQLTableOptions(client.database)
			if opt.TableOptions != "" {
				tx = tx.Set("gorm:table_options", opt.TableOptions)
			}
		}
		err = tx.AutoMigrate(table)
		if err != nil {
			return err
		}
	}
	return nil
}

// StartMonitor Monitor DBState
func (model *DBClient) StartMonitor() {
	db, err := model.conn.DB()
	if err != nil {
		model.conn.Logger.Error(context.Background(), "failed model.conn.DB()")
		return
	}

	for {
		time.Sleep(1 * time.Minute)
		stat := db.Stats()
		msg := fmt.Sprintf("maxopen : %d , opened:  %d , idle : %d ,  inuse: %d , wait : %d,  waitduration : %s",
			stat.MaxOpenConnections, stat.OpenConnections, stat.Idle, stat.InUse, stat.WaitCount,
			stat.WaitDuration.String(),
		)
		model.conn.Logger.Info(context.Background(), msg)
	}
}

// SetLogger set logger
func (c *DBClient) SetLogger(writer io.Writer, config *logger.Config) {
	if config == nil {
		config = &logger.Config{
			SlowThreshold:             3 * time.Second, // 慢 SQL 阈值
			LogLevel:                  logger.Error,    // 日志级别
			IgnoreRecordNotFoundError: true,            // 忽略ErrRecordNotFound（记录未找到）错误
			Colorful:                  false,           // 禁用彩色打印
		}
		l, ok := LogLevelMap[c.options.SQLLevel]
		if ok {
			config.LogLevel = l
		}

	}
	newLogger := logger.New(
		log.New(writer, "gorm", log.LstdFlags), // io writer（日志输出的目标，前缀和日志包含的内容——译者注）
		*config,
	)
	c.conn.Config.Logger = newLogger
}

// ConfigOptions Config client options
// support key in dbclientoptions
func (c *DBClient) ConfigOptions(options DBClientOptions) error {
	db, err := c.conn.DB()
	if err != nil {
		return err
	}
	db.SetMaxOpenConns(options.MaxOpenConn)
	db.SetMaxIdleConns(options.MaxIdleConn)
	c.SetLogger(os.Stdout, nil)
	return nil
}
