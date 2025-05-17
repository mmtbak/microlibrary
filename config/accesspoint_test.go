package config

import (
	"testing"

	"gopkg.in/go-playground/assert.v1"
)

func TestAccessPoint(t *testing.T) {

	type Param struct {
		MaxOpenConn int
		MaxIdleConn int
		LogLevel    string
	}

	config := AccessPoint{
		Source: "mysql://root:password@tcp(127.0.0.1:3306)/mydb_test?charset=utf8&parseTime=true&loc=Local",
		Options: map[string]interface{}{
			"maxopenconn": 1000,
			"maxidleconn": 1000,
			"loglevel":    "debug",
		},
	}
	expectParam := Param{
		MaxOpenConn: 1000,
		MaxIdleConn: 1000,
		LogLevel:    "debug",
	}
	expectDSN := DSN{
		RAW:       "mysql://root:password@tcp(127.0.0.1:3306)/mydb_test?charset=utf8&parseTime=true&loc=Local",
		Source:    "root:password@tcp(127.0.0.1:3306)/mydb_test?charset=utf8&parseTime=true&loc=Local",
		Scheme:    "mysql",
		User:      "root",
		Password:  "password",
		Host:      "127.0.0.1",
		Port:      "3306",
		Path:      "mydb_test",
		Params:    map[string]string{"charset": "utf8", "parseTime": "true", "loc": "Local"},
		Transport: "tcp",
	}
	var param Param
	dsn := config.Decode()
	err := config.DecodeOption(&param)
	assert.Equal(t, err, nil)
	assert.Equal(t, param, expectParam)
	// assert.Equal(t, dsn, expectDSN)
	assert.Equal(t, dsn.Host, expectDSN.Host)
	assert.Equal(t, dsn.Port, expectDSN.Port)
	assert.Equal(t, dsn.Path, expectDSN.Path)
	assert.Equal(t, dsn.Source, expectDSN.Source)
	assert.Equal(t, dsn.Params["charset"], expectDSN.Params["charset"])
	assert.Equal(t, dsn.Params["parseTime"], expectDSN.Params["parseTime"])
	assert.Equal(t, dsn.Params["loc"], expectDSN.Params["loc"])

}
