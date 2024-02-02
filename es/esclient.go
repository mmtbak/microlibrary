package es

import (
	"context"
	"fmt"
	"log"

	"github.com/mmtbak/microlibrary/config"
	"github.com/olivere/elastic/v7"
)

// ESClient es连接
type ESClient struct {
	conn   *elastic.Client
	logger log.Logger
	conf   config.AccessPoint
}

// ESClientOptions  配置选项
type ESClientOptions struct {
	MaxOpenConn  int
	MaxIdleConn  int
	MaxIdleTime  string
	EnableSQLLog bool
}

// NewESClient NewESClient
func NewESClient(conf config.AccessPoint) (*ESClient, error) {

	op := ESClientOptions{
		MaxOpenConn:  100,
		MaxIdleConn:  100,
		EnableSQLLog: false,
	}

	dsn, err := conf.Decode(&op)
	if err != nil {
		return nil, err
	}
	url := fmt.Sprintf("http://%s:%s", dsn.Host, dsn.Port)
	fmt.Println(url)
	client, err := elastic.NewClient(
		//elastic 服务地址
		elastic.SetURL(url),
		// 设置错误日志输出
		elastic.SetSniff(false),
		elastic.SetHealthcheck(false),
		elastic.SetBasicAuth(dsn.User, dsn.Password),
	)
	if err != nil {
		return nil, err
	}
	info, code, err := client.Ping(url).Do(context.Background())
	if err != nil {
		return nil, err
	}
	fmt.Println(info, code)

	return &ESClient{
		conn: client,
		conf: conf,
	}, nil
}

// Client Client
func (es ESClient) Client() *elastic.Client {
	return es.conn
}
