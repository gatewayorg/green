package cache

import (
	"testing"

	"github.com/sunvim/utils/tools"
)

func TestGetSet(t *testing.T) {
	c := New(10240)
	key := []byte("hello")
	val := []byte("world")
	c.Set(key, val)
	rsp := c.Get(nil, key)
	if tools.BytesToStringFast(rsp) != tools.BytesToStringFast(val) {
		t.Errorf("want: %s got: %s", tools.BytesToStringFast(val), tools.BytesToStringFast(rsp))
	}
}
