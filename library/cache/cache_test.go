package cache

import (
	"bytes"
	"testing"
	"time"

	"github.com/mmtbak/microlibrary/library/config"
)

func TestCache(t *testing.T) {
	var conf = config.AccessPoint{
		Source: "freecache://localhost",
		Options: map[string]interface{}{
			"sizekb":      1000,
			"expirehours": 2,
		},
	}
	c, err := NewCache(conf)
	if err != nil {
		t.Fatal(err)
	}
	key := []byte("key1")
	value := []byte("value1")
	err = c.Set(key, value)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(4 * time.Second)
	c.Active(key)
	time.Sleep(4 * time.Second)
	v, err := c.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(v, value) == false {
		t.Fatal("value not equal")
	}
}
