package redisdb

import (
	"encoding/json"
	"testing"

	"github.com/herb-go/herbdata/kvdb"
	"github.com/herb-go/herbdata/kvdb/featuretestutil"
)

func TestDriver(t *testing.T) {
	featuretestutil.TestDriver(func() kvdb.Driver {
		c := &Config{}
		err := json.Unmarshal([]byte(testConfig), c)
		if err != nil {
			panic(err)
		}
		d, err := c.CreateDriver()
		if err != nil {
			panic(err)
		}
		conn := (d.(*Driver)).Pool.Get()
		defer conn.Close()
		conn.Send("FLUSHDB")
		return d
	},
		t.Fatal)
}
