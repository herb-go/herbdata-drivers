package freecachedb

import (
	"errors"
	"testing"

	"github.com/coocood/freecache"

	"github.com/herb-go/herbdata"
	"github.com/herb-go/herbdata/kvdb"
	"github.com/herb-go/herbdata/kvdb/featuretestutil"
)

func TestDriver(t *testing.T) {
	featuretestutil.TestDriver(func() kvdb.Driver {
		d, err := (&Config{Size: 500000}).CreateDriver()
		if err != nil {
			panic(err)
		}
		return d
	},
		t.Fatal)
}

func TestError(t *testing.T) {
	var newerr error
	newerr = convertError(nil)
	if newerr != nil {
		t.Fatal(newerr)
	}
	newerr = convertError(freecache.ErrLargeKey)
	if newerr != herbdata.ErrKeyTooLarge {
		t.Fatal(newerr)
	}
	newerr = convertError(freecache.ErrLargeEntry)
	if newerr != herbdata.ErrEntryTooLarge {
		t.Fatal(newerr)
	}
	newerr = convertError(errors.New("test"))
	if newerr.Error() != "test" {
		t.Fatal(newerr)
	}
}
