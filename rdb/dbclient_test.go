package rdb

import (
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

func TestConnectMySQL(t *testing.T) {

	dbConfig := &Config{
		DSN:          "mysql://root:root@tcp(127.0.0.1:3306)/testdb?charset=utf8&parseTime=true&loc=Local",
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
