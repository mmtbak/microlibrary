package rdb

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql" // mysql driver
	"github.com/mmtbak/microlibrary/config"
	"gorm.io/driver/clickhouse"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var schemas = struct {
	MySQL      string
	Clickhouse string
}{
	MySQL:      "mysql",
	Clickhouse: "clickhouse",
}

// DBClient Gorm的数据库连接.
type DBClient struct {
	db     *gorm.DB
	config *Config
}

// Config DBClient配置.
type Config struct {
	Scheme      string
	Source      string
	MaxOpenConn int
	MaxIdleConn int
	MaxIdleTime time.Duration
	LogLevel    logger.LogLevel
	Cluster     string
}

// DBClientOptions DBClient 配置选项.
type DBClientOptions struct {
	MaxOpenConn int
	MaxIdleConn int
	MaxIdleTime string
	LogLevel    string
	Cluster     string
}

var defaultDBClientOptions = DBClientOptions{
	MaxOpenConn: 100,
	MaxIdleConn: 100,
	MaxIdleTime: "",
	LogLevel:    "info",
}

// LogLevelMap log level for config string.
var LogLevelMap = map[string]logger.LogLevel{
	"warn":   logger.Warn,
	"info":   logger.Info,
	"error":  logger.Error,
	"silent": logger.Silent,
}

func NewConfig() *Config {
	return &Config{}
}

// ParseConfig Parse config from accesspoint.
func ParseConfig(conf config.AccessPoint) (config *Config, err error) {
	clientoption := defaultDBClientOptions
	config = &Config{}

	dsn := conf.Decode()
	err = conf.DecodeOption(&clientoption)
	if err != nil {
		return nil, err
	}
	dsn.Scheme = strings.ToLower(dsn.Scheme)

	// 设置日志级别

	loglevel, ok := LogLevelMap[clientoption.LogLevel]
	if !ok {
		err = fmt.Errorf("unsupported loglevel : [ %s ]", clientoption.LogLevel)
		return
	}
	// detect db type
	switch dsn.Scheme {
	case schemas.MySQL:
		if !strings.Contains(dsn.Source, "charset") {
			dsn.Source += "&charset=utf8&parseTime=true&loc=Local"
		}
	case schemas.Clickhouse:
		if !strings.Contains(dsn.Source, "read_timeout") {
			dsn.Source += "&read_timeout=10s"
		}
		if !strings.Contains(dsn.Source, "dial_timeout") {
			dsn.Source += "&dial_timeout=10s"
		}
	default:
		err = fmt.Errorf("unsupported database type : [ %s ]", dsn.Scheme)
		return
	}

	config = &Config{
		Scheme:      dsn.Scheme,
		Source:      dsn.Source,
		LogLevel:    loglevel,
		MaxOpenConn: clientoption.MaxOpenConn,
		MaxIdleConn: clientoption.MaxIdleConn,
		Cluster:     clientoption.Cluster,
	}

	if clientoption.MaxIdleTime != "" {
		maxidletime, err := time.ParseDuration(clientoption.MaxIdleTime)
		if err != nil {
			return nil, err
		}
		config.MaxIdleTime = maxidletime
	}
	return config, nil
}

// Open Open database connection.
func Open(config *Config) (conn *gorm.DB, err error) {

	// 设置日志级别
	sqllogger := logger.Default
	sqllogger.LogMode(config.LogLevel)

	gormOption := &gorm.Config{
		Logger: sqllogger,
	}

	switch config.Scheme {
	case schemas.MySQL:
		conn, err = gorm.Open(mysql.Open(config.Source), gormOption)
	case schemas.Clickhouse:
		conn, err = gorm.Open(clickhouse.Open(config.Source), gormOption)
	default:
		err = fmt.Errorf("unsupported database type : [ %s ]", config.Scheme)
		return
	}
	if err != nil {
		return nil, err
	}

	db, err := conn.DB()
	if err != nil {
		return
	}

	// 设置连接池
	if config.MaxIdleTime > 0 {
		db.SetConnMaxIdleTime(config.MaxIdleTime)
	}
	if config.MaxIdleConn > 0 {
		db.SetMaxIdleConns(config.MaxIdleConn)
	}
	if config.MaxOpenConn > 0 {
		db.SetMaxOpenConns(config.MaxOpenConn)
	}

	return conn, nil
}

// NewDBClient Create DBEngine instance.
func NewDBClient(config *Config) (*DBClient, error) {

	conn, err := Open(config)
	if err != nil {
		return nil, err
	}
	client := &DBClient{
		db:     conn,
		config: config,
	}
	return client, nil
}

// WithDB set client db connection
func (client *DBClient) WithDB(db *gorm.DB) *DBClient {
	client.db = db
	if client.config == nil {
		client.config = NewConfig()
	}
	return client
}

// DB DB.
func (client *DBClient) DB() *gorm.DB {
	return client.db
}

// Session create a new session.
func (client *DBClient) Session() *gorm.DB {
	return client.db.Session(&gorm.Session{})
}

// NewTx create a new transaction.
func (client *DBClient) NewTx() *gorm.DB {
	return client.db.Begin()
}

// NewTxMaker create a tx maker.
func (client *DBClient) NewTxMaker(tx Tx) (Tx, *TxMaker) {
	return NewTxMaker(tx, client)
}

// SyncTables sync tables defined in  table object.
func (client *DBClient) SyncTables(tables []any) error {
	var err error
	dbop := DataBaseOption{
		Cluster:      client.config.Cluster,
		DatabaseName: client.db.Migrator().CurrentDatabase(),
	}
	for _, table := range tables {
		tx, maker := NewTxMaker(nil, client)
		defer maker.Close(&err)
		var opt TableOption

		// 如果DB是clickhouse ， 则尝试解析clickhouse tableoption
		if client.config.Scheme == schemas.Clickhouse {
			// 发现确实有clickhouse的tableoption，则解析tableoption
			if cktable, ok := table.(ClickhouseTable); ok {
				opt = cktable.ClickhouseTableOption(dbop)
				if opt.TableOptions != "" {
					tx = tx.Set("gorm:table_options", opt.TableOptions)
				}
				if opt.TableClusterOptions != "" {
					tx = tx.Set("gorm:table_cluster_options", opt.TableClusterOptions)
				}
			}
			// 如果DB是mysql ，则尝试解析mysql tableoption
		} else if client.config.Scheme == schemas.MySQL {
			// 发现确实db是mysql，则解析tableoption
			if mytable, ok := table.(MySQLTable); ok {
				opt = mytable.MySQLTableOption(dbop)
				if opt.TableOptions != "" {
					tx = tx.Set("gorm:table_options", opt.TableOptions)
				}
			}
		}
		err = tx.AutoMigrate(table)
		if err != nil {
			return err
		}
	}
	return nil
}

// DropTables drop  tables from  db.
func (client *DBClient) DropTables(tables []any) error {
	return client.DB().Migrator().DropTable(tables...)
}

// StartMonitor Monitor DBState.
func (client *DBClient) Stats() (sql.DBStats, error) {

	db, err := client.db.DB()
	if err != nil {
		return sql.DBStats{}, err
	}
	return db.Stats(), nil
}
