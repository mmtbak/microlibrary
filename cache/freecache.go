package cache

import (
	"strconv"
	"time"

	"github.com/coocood/freecache"
	"github.com/mmtbak/microlibrary/config"
)

// default sizeKB 10MB.
const defaultcachesizekb = 10 * 1024

// FreeCache set.
type FreeCache struct {
	cache  *freecache.Cache
	params freecacheParam
}

type freecacheParam struct {
	SizeKB int
}

// NewFreecache free cache.
func NewFreecache(conf config.AccessPoint) (*FreeCache, error) {
	var err error
	param := freecacheParam{
		SizeKB: defaultcachesizekb,
	}

	dsndata := conf.Decode()
	params := dsndata.Params
	if sizekb, ok := params["sizekb"]; ok {
		param.SizeKB, err = strconv.Atoi(sizekb)
		if err != nil {
			return nil, err
		}
	}
	cache := freecache.NewCache(param.SizeKB * 1024)
	return &FreeCache{cache: cache, params: param}, nil
}

// Get key to cache.
func (c *FreeCache) Get(key []byte) (value []byte, err error) {
	return c.cache.Get(key)
}

// Set key to cache.
func (c *FreeCache) Set(key, value []byte, expiration time.Duration) error {
	return c.cache.Set(key, value, int(expiration.Seconds()))
}

// Delete key to cache.
func (c *FreeCache) Delete(key []byte) bool {
	return c.cache.Del(key)
}

// Active key to cache.
func (c *FreeCache) Active(key []byte, expiration time.Duration) error {
	return c.cache.Touch(key, int(expiration.Seconds()))
}
