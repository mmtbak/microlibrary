package rdb

import (
	"testing"

	"github.com/mmtbak/microlibrary/config"
	"gopkg.in/go-playground/assert.v1"
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
					"MaxIdleConn": 100,
					"MaxOpenConn": 100,
					"Loglevel":    "info",
				},
			},
			wantError: false,
			except: &Config{
				Scheme:      "mysql",
				Source:      "root:password@tcp(127.0.0.1:3306)/my_db?charset=utf8&parseTime=true&loc=Local",
				MaxOpenConn: 100,
				MaxIdleConn: 100,
				LogLevel:    logger.Info,
				Cluster:     "",
			},
		},
		{
			config: config.AccessPoint{
				Source: "clickhouse://root:password@127.0.0.1:9000/mydb?read_timeout=10s",
				Options: map[string]interface{}{
					"maxopenconn": 1000,
					"maxidleconn": 1000,
					"loglevel":    "error",
					"cluster":     "defaultcluster",
				},
			},
			wantError: false,
			except: &Config{
				Scheme:      "clickhouse",
				Source:      "root:password@127.0.0.1:9000/mydb?read_timeout=10s&dial_timeout=10s",
				MaxOpenConn: 1000,
				MaxIdleConn: 1000,
				LogLevel:    logger.Error,
				Cluster:     "defaultcluster",
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
