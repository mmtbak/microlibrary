// Package cache 提供缓存的能力
package cache

import (
	"fmt"
	"time"

	"github.com/mmtbak/microlibrary/config"
)

// Cache interface
type Cache interface {
	// set 填入key/value
	Set(key, value []byte, expiration time.Duration) error
	// get 根据key 查询value
	Get(key []byte) (value []byte, err error)
	// delete 删除key
	Delete(key []byte) bool
	// Active 激活key ,刷新key的过期时间
	Active(key []byte, expiration time.Duration) error
}

// NewCache new cache
func NewCache(conf config.AccessPoint) (Cache, error) {
	dsn, err := conf.Decode(nil)
	if err != nil {
		return nil, err
	}
	switch dsn.Scheme {
	case "freecache":
		return NewFreecache(conf)
	default:
		err = fmt.Errorf("cache: unsupported schema '%s'", dsn.Scheme)
	}
	return nil, err
}
