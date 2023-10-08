package cache

import (
	"github.com/coocood/freecache"
	"github.com/mmtbak/microlibrary/library/config"
)

// expireHours  3600 seconds
const expireHours = 1

// sizeKB 10MB
const sizeKB = 10 * 1024

// FreeCache set
type FreeCache struct {
	cache *freecache.Cache
	op    cacheOption
}

type cacheOption struct {
	SizeKB      int
	ExpireHours int
}

// NewFreecache free cache
func NewFreecache(conf config.AccessPoint) (*FreeCache, error) {
	var op = cacheOption{
		ExpireHours: expireHours,
		SizeKB:      sizeKB,
	}
	_, err := conf.Decode(&op)
	if err != nil {
		return nil, err
	}
	cache := freecache.NewCache(op.SizeKB * 1024)
	// debug.SetGCPercent(20)
	return &FreeCache{cache: cache, op: op}, nil
}

// Get key to cache
func (c *FreeCache) Get(key []byte) (value []byte, err error) {
	return c.cache.Get(key)
}

// Set key to cache
func (c *FreeCache) Set(key, value []byte) error {
	return c.cache.Set(key, value, c.op.ExpireHours*3600)
}

// Delete key to cache
func (c *FreeCache) Delete(key []byte) bool {
	return c.cache.Del(key)
}

// Active key to cache
func (c *FreeCache) Active(key []byte) error {
	return c.cache.Touch(key, c.op.ExpireHours*3600)
}
