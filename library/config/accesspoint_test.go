package config

import (
	"fmt"
	"testing"
)

func TestAccessPoint(t *testing.T) {

	testcases := []struct {
		data  AccessPoint
		param interface{}
	}{
		{
			data: AccessPoint{
				Source: "mysql://root:password@tcp(128.0.0.1:3306)/mydb_test?charset=utf8&parseTime=true&loc=Local",
				Options: map[string]interface{}{
					"maxopenconn": 1000,
					"maxidleconn": 1000,
					"sqllog":      true,
				},
			},
			param: struct {
				MaxOpenConn int
				MaxIdleConn int
				SQLLog      bool
			}{},
		},
	}

	for i, testcase := range testcases {
		fmt.Println("idx :", i)
		dsn, err := testcase.data.Decode(&testcase.param, nil)
		if err != nil {
			fmt.Println(err)
			continue
		}
		fmt.Println("dsn: ", dsn)
		fmt.Println("dsn.source : ", dsn.Source)
		fmt.Println("option: ", testcase.param)

	}
}
