package config

import (
	"fmt"
	"strings"
)

// ConfigLoader 配置中心load接口类的定义.
type ConfigLoader interface {
	// loadtype
	Type() string
	// Load 从配置中心中获得配置需要的配置值
	Load(val interface{}) error
}

// TypeCodec 编码格式.
type TypeCodec string

// Codecs common codec type.
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

// NewConfigLoader  创建一个configloader.
func NewConfigLoader(dsn string) (ConfigLoader, error) {
	var err error
	schemes := strings.SplitN(dsn, "+", 2)
	scheme := schemes[0]
	var source string
	if len(schemes) == 1 {
		source = dsn
	} else {
		source = schemes[1]
	}

	switch scheme {
	case Filetype:
		return NewFileLoader(source)
	default:
		err = fmt.Errorf("config loader:unsupported scheme '%s'", scheme)
	}
	return nil, err
}
