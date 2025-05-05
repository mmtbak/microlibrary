package config

import (
	"fmt"
	"testing"

	"gopkg.in/go-playground/assert.v1"
)

func TestConfigParser(t *testing.T) {

	var testcases = []struct {
		source    string
		protocol  string
		wantError bool
	}{
		{
			source:    "file://.",
			wantError: true,
			protocol:  "file",
		},
		{
			source:    "file://config.yaml",
			protocol:  "file",
			wantError: true,
		},
		{
			source:    "file:///etc/config.toml",
			protocol:  "file",
			wantError: true,
		},
		{
			source:    "file://example/config.yaml",
			protocol:  "file",
			wantError: false,
		},
		{
			source:    "file://config.json",
			protocol:  "file",
			wantError: true,
		},
		{
			source:    "file://../config/example/config.yaml",
			protocol:  "file",
			wantError: false,
		},
		{
			source:    "nacos://127.0.0.1:9000/",
			protocol:  "nacos",
			wantError: true,
		},
	}

	for idx, tt := range testcases {
		fmt.Println("idx :--", idx)
		cfg, err := NewConfigLoader(tt.source)
		fmt.Println(err)
		assert.Equal(t, err != nil, tt.wantError)
		if err != nil {
			continue
		}
		assert.Equal(t, cfg.Type(), tt.protocol)

	}

}

func TestLoadConfig(t *testing.T) {

	var err error
	var testcases = []struct {
		source    string
		config    interface{}
		wantError bool
	}{
		{
			source:    "file://example/config.yaml",
			config:    &config{},
			wantError: false,
		},
	}

	for idx, tt := range testcases {
		fmt.Println("idx :--", idx)

		err = LoadConfig(tt.source, tt.config)
		fmt.Println(err)
		assert.Equal(t, err != nil, tt.wantError)
		if err != nil {
			continue
		}
		fmt.Println(tt.config)
	}

}
