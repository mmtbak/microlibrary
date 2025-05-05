package config

import (
	"github.com/kos-v/dsnparser"
	"github.com/mitchellh/mapstructure"
)

// AccessPoint  general access config
type AccessPoint struct {
	Source  string
	Options map[string]interface{}
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
func (p AccessPoint) Decode(op interface{}) (DSN, error) {

	var err error
	if op != nil {
		err = mapstructure.Decode(p.Options, op)
		if err != nil {
			return DSN{}, err
		}
	}
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
	return dsn, nil
}
