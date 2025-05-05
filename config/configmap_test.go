package config

import (
	"strconv"
	"testing"
)

type Config1 struct {
	Size   int
	Limit  int
	Enable bool
}

func TestParseConfig(t *testing.T) {
	configmap := map[string]string{
		"size":   "10",
		"limit":  "100",
		"enable": "true",
	}

	var configmapfuncs = map[string]func(*Config1, string) error{
		"size": func(c *Config1, v string) error {
			c.Size, _ = strconv.Atoi(v)
			return nil
		},
		"limit": func(c *Config1, v string) error {
			c.Limit, _ = strconv.Atoi(v)
			return nil
		},
		"enable": func(c *Config1, v string) error {
			c.Enable, _ = strconv.ParseBool(v)
			return nil
		},
	}

	var conf = Config1{
		Size:  20,
		Limit: 10,
	}
	err := ParseMapStringConfig(&conf, configmap, configmapfuncs)
	if err != nil {
		t.Fatal(err)
	}

	if conf.Size != 10 {
		t.Fatal("size")
	}
	if conf.Limit != 100 {
		t.Fatal("limit")
	}
	if conf.Enable != true {
		t.Fatal("enable")
	}
}
