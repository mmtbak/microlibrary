package rdb

import (
	"context"

	"log/slog"

	"github.com/mmtbak/microlibrary/config"
	"github.com/qiniu/qmgo"
)

// MongoClient mongodb client.
type MongoClient struct {
	conn     *qmgo.QmgoClient
	config   config.AccessPoint
	logger   slog.Logger
	Database string
}

// NewMongoClient Create DBEngine instance.
func NewMongoClient(conf config.AccessPoint) (*MongoClient, error) {
	dsn := conf.Decode()
	conn, err := qmgo.Open(context.Background(), &qmgo.Config{Uri: conf.Source, Database: dsn.Path})
	if err != nil {
		return nil, err
	}

	clt := &MongoClient{
		conn:     conn,
		config:   conf,
		Database: dsn.Path,
	}

	return clt, nil
}

// SetLogger set logger.
func (client *MongoClient) SetLogger(logger slog.Logger) {
	client.logger = logger
}

// Conn return connection.
func (client *MongoClient) Conn() *qmgo.QmgoClient {
	return client.conn
}

// DB return db.
func (client *MongoClient) DB() *qmgo.Database {
	return client.conn.Client.Database(client.Database)
}
