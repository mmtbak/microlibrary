package config

import (
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
			wantError: false,
			protocol:  "file",
		},
		{
			source:    "file://config.yaml",
			wantError: false,
			protocol:  "file",
		},
		{
			source:    "file:///etc/config.toml",
			wantError: false,
			protocol:  "file",
		},
		{
			source:    "file://config.json",
			wantError: false,
			protocol:  "file",
		},
		{
			source:    "nacos://127.0.0.1:9000/",
			wantError: false,
			protocol:  "nacos",
		},
	}

	for _, tt := range testcases {
		t.Run(tt.source, func(t *testing.T) {
			conf, err := NewConfigLoader(tt.source)
			assert.Equal(t, err == nil, tt.wantError)
			if err != nil {
				return
			}
			assert.Equal(t, conf.Type(), tt.protocol)
		})
	}
}
