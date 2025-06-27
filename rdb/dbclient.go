package rdb

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql" // mysql driver
	"github.com/mmtbak/dsnparser"
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
	schema string // 数据库类型
	config *Config
}

// Config DBClient配置.
type Config struct {
	DSN          string `json:"dsn" yaml:"dsn"` // 数据库连接字符串
	MaxOpenConns int    `json:"max_open_conns"  yaml:"max_open_conns"`
	MaxIdleConns int    `json:"max_idle_conns" yaml:"max_idle_conns"`
	MaxIdleTime  string `json:"max_idle_time" yaml:"max_idle_time"` // 连接池最大空闲时间
	LogLevel     string `json:"log_level" yaml:"log_level"`         // 日志级别
	Cluster      string `json:"cluster" yaml:"cluster"`             // 集群名称
}

// DBClientOptions DBClient 配置选项.
type DBClientOptions struct {
	MaxOpenConns int
	MaxIdleConns int
	MaxIdleTime  string
	LogLevel     string
	Cluster      string
}

var defaultDBClientOptions = DBClientOptions{
	MaxOpenConns: 100,
	MaxIdleConns: 100,
	MaxIdleTime:  "",
	LogLevel:     "info",
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
		DSN:          dsn.RAW,
		LogLevel:     clientoption.LogLevel,
		MaxOpenConns: clientoption.MaxOpenConns,
		MaxIdleConns: clientoption.MaxIdleConns,
		MaxIdleTime:  clientoption.MaxIdleTime,
		Cluster:      clientoption.Cluster,
	}

	if clientoption.MaxIdleTime != "" {
		_, err := time.ParseDuration(clientoption.MaxIdleTime)
		if err != nil {
			return nil, err
		}
		config.MaxIdleTime = clientoption.MaxIdleTime
	}
	return config, nil
}

// Open Open database connection.
func Open(config *Config) (conn *gorm.DB, err error) {

	// 设置日志级别
	sqllogger := logger.Default

	if config.LogLevel != "" {
		if loglevel, ok := LogLevelMap[config.LogLevel]; ok {
			sqllogger.LogMode(loglevel)
		} else {
			err = fmt.Errorf("unsupported log level : [ %s ]", config.LogLevel)
			return
		}
	}

	gormOption := &gorm.Config{
		Logger: sqllogger,
	}
	dsnData := dsnparser.Parse(config.DSN)

	schema := dsnData.GetScheme()
	source := dsnData.GetSource()
	switch schema {
	case schemas.MySQL:
		conn, err = gorm.Open(mysql.Open(source), gormOption)
	case schemas.Clickhouse:
		conn, err = gorm.Open(clickhouse.Open(source), gormOption)
	default:
		err = fmt.Errorf("unsupported database type : [ %s ]", schema)
		return
	}
	if err != nil {
		return nil, err
	}

	db, err := conn.DB()
	if err != nil {
		return
	}
	if config.MaxIdleConns > 0 {
		db.SetMaxIdleConns(config.MaxIdleConns)
	}
	if config.MaxOpenConns > 0 {
		db.SetMaxOpenConns(config.MaxOpenConns)
	}

	if config.MaxIdleTime != "" {
		maxIdleTime, err := time.ParseDuration(config.MaxIdleTime)
		if err != nil {
			return nil, err
		}
		// 设置连接池
		if maxIdleTime > 0 {
			db.SetConnMaxIdleTime(maxIdleTime)
		}
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
		schema: conn.Name(),
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
		if client.schema == schemas.Clickhouse {
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
		} else if client.schema == schemas.MySQL {
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
