package cache

import (
	"bytes"
	"testing"
	"time"

	"github.com/coocood/freecache"
	"github.com/mmtbak/microlibrary/config"
	"gopkg.in/go-playground/assert.v1"
)

func TestFreecache(t *testing.T) {
	conf := config.AccessPoint{
		Source: "freecache://localhost/?sizekb=1000",
	}
	c, err := NewFreecache(conf)
	assert.Equal(t, err, nil)
	expireduration := 2 * time.Second
	key := []byte("key1")
	value := []byte("value1")
	err = c.Set(key, value, expireduration)
	assert.Equal(t, err, nil)
	time.Sleep(1 * time.Second)
	v, err := c.Get(key)
	assert.Equal(t, err, nil)
	assert.Equal(t, bytes.Equal(v, value), true)
	c.Active(key, expireduration)
	time.Sleep(2 * time.Second)
	v, err = c.Get(key)
	assert.Equal(t, err, freecache.ErrNotFound)
	assert.Equal(t, bytes.Equal(v, value), false)
}
