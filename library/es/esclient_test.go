package es

import (
	"fmt"
	"testing"

	"github.com/mmtbak/microlibrary/library/config"
)

func TestNewESClient(t *testing.T) {
	var ac = config.AccessPoint{
		Source: "elasticsearch://elastic:password@127.0.0.1:9200/?read_timeout=10s",
		Options: map[string]interface{}{
			"maxopenconn": 1000,
			"maxidleconn": 1000,
			"sqllog":      true,
		},
	}
	esclient, err := NewESClient(ac)
	fmt.Println(esclient, err)

}
