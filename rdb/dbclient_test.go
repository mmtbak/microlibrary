package rdb

import (
	"database/sql"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/mmtbak/microlibrary/config"
	"gopkg.in/go-playground/assert.v1"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestParseConfig(t *testing.T) {
	testcases := []struct {
		config    config.AccessPoint
		wantError bool
		except    *Config
	}{
		{
			config: config.AccessPoint{
				Source: "mysql://root:password@tcp(127.0.0.1:3306)/my_db?charset=utf8&parseTime=true&loc=Local",
				Options: map[string]interface{}{
					"MaxIdleConns": 200,
					"MaxOpenConns": 200,
					"Loglevel":     "info",
				},
			},
			wantError: false,
			except: &Config{
				DSN:          "mysql://root:password@tcp(127.0.0.1:3306)/my_db?charset=utf8&parseTime=true&loc=Local",
				MaxOpenConns: 200,
				MaxIdleConns: 200,
				LogLevel:     "info",
				Cluster:      "",
			},
		},
		{
			config: config.AccessPoint{
				Source: "clickhouse://root:password@127.0.0.1:9000/mydb?read_timeout=10s",
				Options: map[string]interface{}{
					"MaxIdleConns": 1000,
					"MaxOpenConns": 1000,
					"Loglevel":     "error",
					"Cluster":      "defaultcluster",
				},
			},
			wantError: false,
			except: &Config{
				DSN:          "clickhouse://root:password@127.0.0.1:9000/mydb?read_timeout=10s",
				MaxOpenConns: 1000,
				MaxIdleConns: 1000,
				LogLevel:     "error",
				Cluster:      "defaultcluster",
			},
		},
	}

	for _, tc := range testcases {
		config, err := ParseConfig(tc.config)
		assert.Equal(t, err != nil, tc.wantError)
		if err != nil {
			continue
		}
		assert.Equal(t, config, tc.except)
	}
}

type MockStaffTable struct {
	ID   int `gorm:"primaryKey"`
	Name string
	Age  int
}

func TestDBClientTxMaker(t *testing.T) {

	var err error
	db, mock, err := sqlmock.New()
	assert.Equal(t, err, nil)

	// mock sql "select version()"
	mock.ExpectQuery("SELECT VERSION()").WillReturnRows(sqlmock.NewRows([]string{"VERSION()"}).AddRow("5.7.30"))

	gormDB, err := gorm.Open(mysql.New(mysql.Config{
		Conn: db,
	}), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Info),
	})
	assert.Equal(t, err, nil)
	// mock statement
	// insert success
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO `mock_staff_tables`").WillReturnResult(sqlmock.NewResult(10, 1))
	mock.ExpectCommit()

	client := (&DBClient{}).WithDB(gormDB)
	tx, maker := client.NewTxMaker(nil)
	defer maker.Close(&err)

	assert.Equal(t, err, nil)
	assert.NotEqual(t, tx, nil)
	assert.NotEqual(t, maker, nil)
	err = tx.Create(&MockStaffTable{Name: "test", Age: 25}).Error
	tx.Commit()

}

func TestDBClientSyncTables(t *testing.T) {

	var err error
	db, mock, err := sqlmock.New()
	assert.Equal(t, err, nil)

	// mock sql "select version()"
	mock.ExpectQuery("SELECT VERSION()").WillReturnRows(sqlmock.NewRows([]string{"VERSION()"}).AddRow("5.7.30"))

	gormDB, err := gorm.Open(mysql.New(mysql.Config{
		Conn: db,
	}), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Info),
	})
	assert.Equal(t, err, nil)
	// mock statement
	// insert success
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT SCHEMA_NAME from Information_schema.SCHEMATA").
		WillReturnRows(sqlmock.NewRows([]string{"SCHEMA_NAME"}))
	expectedSQL := "CREATE TABLE `mock_staff_tables`"
	mock.ExpectExec(expectedSQL).
		WillReturnResult(sqlmock.NewResult(1, 1))
	client := (&DBClient{}).WithDB(gormDB)
	err = client.SyncTables([]any{&MockStaffTable{}})
	assert.Equal(t, err, nil)
}

func TestTruncateTables(t *testing.T) {
	type User struct {
		ID   uint
		Name string
		Age  int
	}
	type Product struct {
		ID    uint
		Name  string
		Price float64
	}
	var err error
	db, mock, err := sqlmock.New()
	assert.Equal(t, err, nil)
	// mock sql "select version()"
	mock.ExpectQuery("SELECT VERSION()").WillReturnRows(sqlmock.NewRows([]string{"VERSION()"}).AddRow("5.7.30"))
	gormDB, err := gorm.Open(mysql.New(mysql.Config{
		Conn: db,
	}), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Info),
	})
	assert.Equal(t, err, nil)
	// mock statement
	mock.ExpectBegin()
	mock.ExpectExec("TRUNCATE TABLE users").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("TRUNCATE TABLE products").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()
	client := (&DBClient{}).WithDB(gormDB)
	err = client.TruncateTables([]any{&User{}, &Product{}})
	assert.Equal(t, err, nil)
}

func TestNewConfig(t *testing.T) {
	// 调用NewConfig方法
	config := NewConfig()

	// 验证返回的配置不为nil且为默认值
	assert.NotEqual(t, config, nil)
	assert.Equal(t, config.DSN, "")
	assert.Equal(t, config.MaxOpenConns, 0)
	assert.Equal(t, config.MaxIdleConns, 0)
	assert.Equal(t, config.MaxIdleTime, "")
	assert.Equal(t, config.LogLevel, "")
	assert.Equal(t, config.Cluster, "")
}
func TestDBClient_GetConfig(t *testing.T) {
	testcases := []struct {
		name     string
		config   *Config
		expected *Config
	}{
		{
			name: "正常配置",
			config: &Config{
				DSN:          "mysql://root:password@tcp(127.0.0.1:3306)/testdb",
				MaxOpenConns: 100,
				MaxIdleConns: 50,
				MaxIdleTime:  "5m",
				LogLevel:     "info",
				Cluster:      "test-cluster",
			},
			expected: &Config{
				DSN:          "mysql://root:password@tcp(127.0.0.1:3306)/testdb",
				MaxOpenConns: 100,
				MaxIdleConns: 50,
				MaxIdleTime:  "5m",
				LogLevel:     "info",
				Cluster:      "test-cluster",
			},
		},
		{
			name: "空配置",
			config: &Config{
				DSN:          "",
				MaxOpenConns: 0,
				MaxIdleConns: 0,
				MaxIdleTime:  "",
				LogLevel:     "",
				Cluster:      "",
			},
			expected: &Config{
				DSN:          "",
				MaxOpenConns: 0,
				MaxIdleConns: 0,
				MaxIdleTime:  "",
				LogLevel:     "",
				Cluster:      "",
			},
		},
		{
			name: "部分配置",
			config: &Config{
				DSN:          "clickhouse://localhost:9000/default",
				MaxOpenConns: 200,
				MaxIdleConns: 100,
				MaxIdleTime:  "",
				LogLevel:     "error",
				Cluster:      "",
			},
			expected: &Config{
				DSN:          "clickhouse://localhost:9000/default",
				MaxOpenConns: 200,
				MaxIdleConns: 100,
				MaxIdleTime:  "",
				LogLevel:     "error",
				Cluster:      "",
			},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			// 创建DBClient实例
			client := &DBClient{
				config: tc.config,
			}

			// 调用GetConfig方法
			result := client.GetConfig()

			// 验证返回的配置是否正确
			assert.Equal(t, result, tc.expected)
			assert.Equal(t, result.DSN, tc.expected.DSN)
			assert.Equal(t, result.MaxOpenConns, tc.expected.MaxOpenConns)
			assert.Equal(t, result.MaxIdleConns, tc.expected.MaxIdleConns)
			assert.Equal(t, result.MaxIdleTime, tc.expected.MaxIdleTime)
			assert.Equal(t, result.LogLevel, tc.expected.LogLevel)
			assert.Equal(t, result.Cluster, tc.expected.Cluster)
		})
	}
}

func TestDBClient_WithDB(t *testing.T) {
	testcases := []struct {
		name         string
		initialDB    *gorm.DB
		config       *Config
		newDB        *gorm.DB
		expectDB     *gorm.DB
		expectConfig *Config
	}{
		{
			name:      "有配置时替换DB",
			initialDB: nil,
			config: &Config{
				DSN:          "mysql://root:password@tcp(127.0.0.1:3306)/testdb",
				MaxOpenConns: 100,
				MaxIdleConns: 50,
			},
			newDB: func() *gorm.DB {
				db, mock, err := sqlmock.New()
				assert.Equal(t, err, nil)
				mock.ExpectQuery("SELECT VERSION()").WillReturnRows(sqlmock.NewRows([]string{"VERSION()"}).AddRow("5.7.30"))
				gormDB, err := gorm.Open(mysql.New(mysql.Config{
					Conn: db,
				}), &gorm.Config{})
				assert.Equal(t, err, nil)
				return gormDB
			}(),
			expectConfig: &Config{
				DSN:          "mysql://root:password@tcp(127.0.0.1:3306)/testdb",
				MaxOpenConns: 100,
				MaxIdleConns: 50,
			},
		},
		{
			name:      "无配置时创建新配置",
			initialDB: nil,
			config:    nil,
			newDB: func() *gorm.DB {
				db, mock, err := sqlmock.New()
				assert.Equal(t, err, nil)
				mock.ExpectQuery("SELECT VERSION()").WillReturnRows(sqlmock.NewRows([]string{"VERSION()"}).AddRow("5.7.30"))
				gormDB, err := gorm.Open(mysql.New(mysql.Config{
					Conn: db,
				}), &gorm.Config{})
				assert.Equal(t, err, nil)
				return gormDB
			}(),
			expectConfig: &Config{},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			// 创建初始DBClient
			client := &DBClient{
				db:     tc.initialDB,
				config: tc.config,
			}

			// 调用WithDB方法
			result := client.WithDB(tc.newDB)

			// 验证DB是否正确替换
			assert.Equal(t, result.db, tc.newDB)
			assert.Equal(t, result.config, tc.expectConfig)
			assert.Equal(t, result, client) // 应该返回同一个实例
		})
	}
}

func TestDBClient_DB(t *testing.T) {
	testcases := []struct {
		name     string
		db       *gorm.DB
		expected *gorm.DB
	}{
		{
			name: "有DB连接",
			db: func() *gorm.DB {
				db, mock, err := sqlmock.New()
				assert.Equal(t, err, nil)
				mock.ExpectQuery("SELECT VERSION()").WillReturnRows(sqlmock.NewRows([]string{"VERSION()"}).AddRow("5.7.30"))
				gormDB, err := gorm.Open(mysql.New(mysql.Config{
					Conn: db,
				}), &gorm.Config{})
				assert.Equal(t, err, nil)
				return gormDB
			}(),
		},
		{
			name:     "无DB连接",
			db:       nil,
			expected: nil,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			// 创建DBClient实例
			client := &DBClient{
				db: tc.db,
			}

			// 调用DB方法
			result := client.DB()

			// 验证返回的DB是否正确
			assert.Equal(t, result, tc.db)
		})
	}
}

func TestDBClient_Session(t *testing.T) {
	// 创建模拟DB连接
	db, mock, err := sqlmock.New()
	assert.Equal(t, err, nil)

	// mock sql "select version()"
	mock.ExpectQuery("SELECT VERSION()").WillReturnRows(sqlmock.NewRows([]string{"VERSION()"}).AddRow("5.7.30"))

	gormDB, err := gorm.Open(mysql.New(mysql.Config{
		Conn: db,
	}), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Info),
	})
	assert.Equal(t, err, nil)

	// 创建DBClient实例
	client := &DBClient{
		db: gormDB,
	}

	// 调用Session方法
	session := client.Session()

	// 验证返回的session不为nil
	assert.NotEqual(t, session, nil)
}

func TestDBClient_NewTx(t *testing.T) {
	// 创建模拟DB连接
	db, mock, err := sqlmock.New()
	assert.Equal(t, err, nil)

	// mock sql "select version()"
	mock.ExpectQuery("SELECT VERSION()").WillReturnRows(sqlmock.NewRows([]string{"VERSION()"}).AddRow("5.7.30"))

	gormDB, err := gorm.Open(mysql.New(mysql.Config{
		Conn: db,
	}), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Info),
	})
	assert.Equal(t, err, nil)

	// 创建DBClient实例
	client := &DBClient{
		db: gormDB,
	}

	// 调用NewTx方法
	tx := client.NewTx()

	// 验证返回的事务不为nil
	assert.NotEqual(t, tx, nil)
}

func TestDBClient_Stats(t *testing.T) {
	testcases := []struct {
		name        string
		setupClient func() *DBClient
		expectError bool
	}{
		{
			name: "正常获取统计信息",
			setupClient: func() *DBClient {
				db, mock, _ := sqlmock.New()
				mock.ExpectQuery("SELECT VERSION()").WillReturnRows(sqlmock.NewRows([]string{"VERSION()"}).AddRow("5.7.30"))
				gormDB, _ := gorm.Open(mysql.New(mysql.Config{
					Conn: db,
				}), &gorm.Config{})
				return &DBClient{db: gormDB}
			},
			expectError: false,
		},
		{
			name: "无DB连接时返回错误",
			setupClient: func() *DBClient {
				return &DBClient{db: nil}
			},
			expectError: true,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			client := tc.setupClient()

			// 调用Stats方法
			stats, err := client.Stats()

			// 验证错误情况
			assert.Equal(t, err != nil, tc.expectError)

			if !tc.expectError {
				// 验证返回的统计信息不为空
				assert.NotEqual(t, stats, sql.DBStats{})
			}
		})
	}
}

func TestParseConfig_EdgeCases(t *testing.T) {
	testcases := []struct {
		name        string
		config      config.AccessPoint
		expectError bool
		errorMsg    string
	}{
		{
			name: "不支持的数据库类型",
			config: config.AccessPoint{
				Source: "postgres://user:pass@localhost:5432/db",
				Options: map[string]interface{}{
					"MaxIdleConns": 100,
					"MaxOpenConns": 100,
				},
			},
			expectError: true,
			errorMsg:    "unsupported database type : [ postgres ]",
		},
		{
			name: "无效的MaxIdleTime格式",
			config: config.AccessPoint{
				Source: "mysql://root:password@tcp(127.0.0.1:3306)/testdb",
				Options: map[string]interface{}{
					"MaxIdleTime": "invalid-duration",
				},
			},
			expectError: true,
		},
		{
			name: "空DSN",
			config: config.AccessPoint{
				Source: "",
				Options: map[string]interface{}{
					"MaxIdleConns": 100,
				},
			},
			expectError: true,
		},
		{
			name: "MySQL自动添加charset参数",
			config: config.AccessPoint{
				Source: "mysql://root:password@tcp(127.0.0.1:3306)/testdb",
				Options: map[string]interface{}{
					"MaxIdleConns": 100,
					"MaxOpenConns": 100,
				},
			},
			expectError: false,
		},
		{
			name: "Clickhouse自动添加timeout参数",
			config: config.AccessPoint{
				Source: "clickhouse://root:password@127.0.0.1:9000/mydb",
				Options: map[string]interface{}{
					"MaxIdleConns": 100,
					"MaxOpenConns": 100,
				},
			},
			expectError: false,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			config, err := ParseConfig(tc.config)

			// 验证错误情况
			assert.Equal(t, err != nil, tc.expectError)

			if tc.expectError {
				assert.NotEqual(t, err, nil)
				if tc.errorMsg != "" {
					assert.Equal(t, err.Error(), tc.errorMsg)
				}
			} else {
				// 验证配置不为nil
				assert.NotEqual(t, config, nil)
			}
		})
	}
}

func TestOpen_EdgeCases(t *testing.T) {
	testcases := []struct {
		name        string
		config      *Config
		expectError bool
		errorMsg    string
	}{
		{
			name: "不支持的日志级别",
			config: &Config{
				DSN:      "mysql://root:password@tcp(127.0.0.1:3306)/testdb",
				LogLevel: "invalid-level",
			},
			expectError: true,
			errorMsg:    "unsupported log level : [ invalid-level ]",
		},
		{
			name: "无效的DSN格式",
			config: &Config{
				DSN: "invalid-dsn",
			},
			expectError: true,
		},
		{
			name: "不支持的数据库类型",
			config: &Config{
				DSN: "postgres://user:pass@localhost:5432/db",
			},
			expectError: true,
			errorMsg:    "unsupported database type : [ postgres ]",
		},
		{
			name: "无效的MaxIdleTime格式",
			config: &Config{
				DSN:         "mysql://root:password@tcp(127.0.0.1:3306)/testdb",
				MaxIdleTime: "invalid-duration",
			},
			expectError: true,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			conn, err := Open(tc.config)

			// 验证错误情况
			assert.Equal(t, err != nil, tc.expectError)

			if tc.expectError {
				assert.NotEqual(t, err, nil)
				if tc.errorMsg != "" {
					assert.Equal(t, err.Error(), tc.errorMsg)
				}
			} else {
				// 验证连接不为nil
				assert.NotEqual(t, conn, nil)
			}
		})
	}
}

func TestNewDBClient_EdgeCases(t *testing.T) {
	testcases := []struct {
		name        string
		config      *Config
		expectError bool
	}{
		{
			name: "正常创建客户端",
			config: &Config{
				DSN:          "mysql://root:password@tcp(127.0.0.1:3306)/testdb",
				MaxOpenConns: 100,
				MaxIdleConns: 50,
				LogLevel:     "info",
			},
			expectError: false,
		},
		{
			name: "无效配置导致创建失败",
			config: &Config{
				DSN: "invalid-dsn",
			},
			expectError: true,
		},
		{
			name:        "空配置",
			config:      &Config{},
			expectError: true,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			client, err := NewDBClient(tc.config)

			// 验证错误情况
			assert.Equal(t, err != nil, tc.expectError)

			if !tc.expectError {
				// 验证客户端不为nil且配置正确
				assert.NotEqual(t, client, nil)
				assert.Equal(t, client.config, tc.config)
				assert.NotEqual(t, client.db, nil)
			}
		})
	}
}

func TestConnectMySQL(t *testing.T) {

	dbConfig := &Config{
		DSN:          "mysql://root:password@tcp(127.0.0.1:3306)/testdb?charset=utf8&parseTime=true&loc=Local",
		MaxOpenConns: 200,
		MaxIdleConns: 200,
		LogLevel:     "info",
		Cluster:      "",
	}
	dbClient, err := NewDBClient(dbConfig)
	assert.Equal(t, err, nil)
	err = dbClient.SyncTables([]any{&MockStaffTable{}})
	assert.Equal(t, err, nil)
}
