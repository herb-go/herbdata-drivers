package leveldb

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"github.com/herb-go/herbdata"

	"github.com/herb-go/herbdata/kvdb"
	"github.com/herb-go/herbdata/kvdb/featuretestutil"
)

var tmpdir string

var tmpdb []string

func Clean() {
	if tmpdir == "" {
		return
	}
	for _, v := range tmpdb {
		if strings.HasPrefix(v, tmpdir) {
			os.RemoveAll(v)
		}
	}
	tmpdb = []string{}
	os.Remove(tmpdir)
}
func TestDriver(t *testing.T) {
	var err error
	tmpdir, err = ioutil.TempDir("", "")
	if err != nil {
		panic(err)
	}
	defer Clean()
	featuretestutil.TestDriver(func() kvdb.Driver {
		db, err := ioutil.TempDir(tmpdir, "")
		if err != nil {
			panic(err)
		}
		tmpdb = append(tmpdb, db)
		d, err := (&Config{Database: db}).CreateDriver()
		if err != nil {
			panic(err)
		}
		return d
	},
		func(args ...interface{}) { fmt.Println(args...); panic("fatal") })
}

func TestNextOrder(t *testing.T) {
	var err error
	tmpdir, err = ioutil.TempDir("", "")
	if err != nil {
		panic(err)
	}
	defer Clean()
	db, err := ioutil.TempDir(tmpdir, "")
	if err != nil {
		panic(err)
	}
	tmpdb = append(tmpdb, db)
	d, err := (&Config{Database: db}).CreateDriver()
	if err != nil {
		panic(err)
	}
	err = d.Start()
	if err != nil {
		panic(err)
	}
	keys := string("abcdefg")
	for _, v := range keys {
		err = d.Set([]byte{byte(v)}, []byte{byte(v)})
		if err != nil {
			panic(err)
		}
	}

	defer func() {
		err = d.Stop()
		if err != nil {
			panic(err)
		}
	}()
	var result = ""
	var iter []byte
	var data []*herbdata.KeyValue
	for {
		data, iter, err = d.Next(iter, 3)
		if err != nil {
			panic(err)
		}
		for _, v := range data {
			result = result + string(v.Key)
		}
		if len(iter) == 0 {
			break
		}

	}
	if result != "abcdefg" {
		t.Fatal(result)
	}
}

func TestPrevOrder(t *testing.T) {
	var err error
	tmpdir, err = ioutil.TempDir("", "")
	if err != nil {
		panic(err)
	}
	defer Clean()
	db, err := ioutil.TempDir(tmpdir, "")
	if err != nil {
		panic(err)
	}
	tmpdb = append(tmpdb, db)
	d, err := (&Config{Database: db}).CreateDriver()
	if err != nil {
		panic(err)
	}
	err = d.Start()
	if err != nil {
		panic(err)
	}
	keys := string("abcdefg")
	for _, v := range keys {
		err = d.Set([]byte{byte(v)}, []byte{byte(v)})
		if err != nil {
			panic(err)
		}
	}

	defer func() {
		err = d.Stop()
		if err != nil {
			panic(err)
		}
	}()
	var result = ""
	var iter []byte
	var data []*herbdata.KeyValue
	for {
		data, iter, err = d.Prev(iter, 3)
		if err != nil {
			panic(err)
		}
		for _, v := range data {
			result = result + string(v.Key)
		}
		if len(iter) == 0 {
			break
		}

	}
	if result != "gfedcba" {
		t.Fatal(result)
	}
}
