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
			source:   "file://config.yaml",
			protocol: "file",
		},
		{
			source:   "file:///etc/config.toml",
			protocol: "file",
		},
		{
			source:   "file://config.json",
			protocol: "file",
		},
		{
			source:   "nacos://127.0.0.1:9000/",
			protocol: "nacos",
		},
	}

	for _, tt := range testcases {
		_, err := NewConfigLoader(tt.source)
		assert.Equal(t, err != nil, tt.wantError)
		if err != nil {
			continue
		}

	}

}
