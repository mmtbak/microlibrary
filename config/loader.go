package config

import (
	"fmt"

	"github.com/kos-v/dsnparser"
)

// ConfigLoader 配置中心load接口类的定义
type ConfigLoader interface {
	// loadtype
	Type() string
	// Load 从配置中心中获得配置需要的配置值
	Load(val interface{}) error
}

// TypeCodec 编码格式
type TypeCodec string

// Codecs common codec type
var Codecs = struct {
	YAML TypeCodec
	JSON TypeCodec
	TOML TypeCodec
}{
	YAML: "yaml",
	JSON: "json",
	TOML: "toml",
}

const (
	Filetype  = "file"
	Nacostype = "nacos"
)

// NewConfigLoader  创建一个configloader
func NewConfigLoader(dsn string) (ConfigLoader, error) {

	var err error

	dsndata := dsnparser.Parse(dsn)
	schema := dsndata.GetScheme()

	switch schema {
	case Filetype:
		return NewFileLoader(dsn)
	default:
		err = fmt.Errorf("config loader:unsupported scheme '%s'", schema)
	}
	return nil, err
}

// LoadConfig 从配置中心获得配置
func LoadConfig(dsn string, value interface{}) error {
	loader, err := NewConfigLoader(dsn)
	if err != nil {
		return err
	}
	return loader.Load(value)
}
