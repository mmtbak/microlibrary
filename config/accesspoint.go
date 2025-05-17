package config

import (
	"github.com/mitchellh/mapstructure"
	"github.com/mmtbak/dsnparser"
)

// AccessPoint  general access config
type AccessPoint struct {
	Source  string
	Options map[string]any
}

// DSN container for data after dsn parsing.
type DSN struct {
	RAW       string
	Scheme    string
	Source    string // dsn source : dsn without schema
	User      string
	Password  string
	Host      string
	Port      string
	Hostport  string
	Path      string
	Params    map[string]string
	Transport string
}

// Decode decode
func (p AccessPoint) Decode() DSN {
	dsndata := dsnparser.Parse(p.Source)
	dsn := DSN{
		RAW:       dsndata.GetRaw(),
		Scheme:    dsndata.GetScheme(),
		Source:    dsndata.GetSource(),
		User:      dsndata.GetUser(),
		Password:  dsndata.GetPassword(),
		Host:      dsndata.GetHost(),
		Port:      dsndata.GetPort(),
		Hostport:  dsndata.GetHostPort(),
		Path:      dsndata.GetPath(),
		Params:    dsndata.GetParams(),
		Transport: dsndata.GetTransport(),
	}
	return dsn
}

func (p AccessPoint) DecodeOption(op any) error {
	return mapstructure.Decode(p.Options, op)
}
