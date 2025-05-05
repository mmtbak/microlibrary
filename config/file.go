package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/BurntSushi/toml"
	"github.com/kos-v/dsnparser"
	"gopkg.in/yaml.v3"
)

// FileLoader 从文件中加载配置
type FileLoader struct {
	filepath string
	content  []byte
	option   FileOption
}

type FileOption struct {
	Codec TypeCodec
}

func NewFileLoader(dsn string) (*FileLoader, error) {

	var err error
	var option = FileOption{}
	dsndata := dsnparser.Parse(dsn)
	fppath := filepath.Join(dsndata.GetHostPort(), dsndata.GetPath())
	content, err := os.ReadFile(fppath)
	if err != nil {
		return nil, err
	}
	fileext := filepath.Ext(fppath)
	if fileext == ".json" {
		option.Codec = Codecs.JSON
	} else if fileext == ".yaml" {
		option.Codec = Codecs.YAML
	} else if fileext == ".toml" {
		option.Codec = Codecs.TOML
	}
	err = dsndata.DecodeParams(&option)
	if err != nil {
		return nil, err
	}

	loader := &FileLoader{
		filepath: fppath,
		content:  content,
		option:   option,
	}
	return loader, nil

}

// Load load config into val
func (l *FileLoader) Load(val interface{}) error {

	var err error
	if TypeCodec(l.option.Codec) == Codecs.YAML {
		err = yaml.Unmarshal(l.content, val)
	} else if TypeCodec(l.option.Codec) == Codecs.JSON {
		err = json.Unmarshal(l.content, val)
	} else if TypeCodec(l.option.Codec) == Codecs.TOML {
		_, err = toml.Decode(string(l.content), val)
	} else {
		err = fmt.Errorf("unsupported codec type '%s'", l.option.Codec)
	}
	return err
}

// Type return config type
func (l *FileLoader) Type() string {
	return Filetype
}
