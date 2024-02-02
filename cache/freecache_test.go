package cache

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/mmtbak/microlibrary/config"
)

func TestFreecache(t *testing.T) {
	var conf = config.AccessPoint{
		Source: "freecache://localhost/?sizekb=1000",
	}
	expireduration := 1 * time.Hour
	c, err := NewCache(conf)
	if err != nil {
		t.Fatal(err)
	}
	key := []byte("key1")
	value := []byte("value1")
	err = c.Set(key, value, expireduration)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(4 * time.Second)
	c.Active(key, expireduration)
	time.Sleep(4 * time.Second)
	v, err := c.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(v)
	if bytes.Equal(v, value) == false {
		t.Fatal("value not equal")
	}
}
