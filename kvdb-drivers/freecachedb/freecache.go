package freecachedb

import (
	"github.com/coocood/freecache"
	"github.com/herb-go/herbdata"
	"github.com/herb-go/herbdata/kvdb"
)

func convertError(err error) error {
	if err == nil {
		return err
	}
	if err == freecache.ErrNotFound {
		return herbdata.ErrNotFound
	}
	if err == freecache.ErrLargeKey {
		return herbdata.ErrKeyTooLarge
	}
	if err == freecache.ErrLargeEntry {
		return herbdata.ErrEntryTooLarge
	}
	return err
}

//Driver freecache key-value database driver
type Driver struct {
	kvdb.Nop
	fc *freecache.Cache
}

//Set set value by given key
func (d *Driver) Set(key []byte, value []byte) error {
	return convertError(d.fc.Set(key, value, -1))
}

//SetWithTTL set value by given key and ttl in second.
func (d *Driver) SetWithTTL(key []byte, value []byte, ttlInSecond int64) error {
	if ttlInSecond <= 0 {
		return herbdata.ErrInvalidatedTTL
	}
	return convertError(d.fc.Set(key, value, int(ttlInSecond)))
}

//Get get value by given key
func (d *Driver) Get(key []byte) ([]byte, error) {
	data, err := d.fc.Get(key)
	return data, convertError(err)
}

//Delete delete value by given key
func (d *Driver) Delete(key []byte) error {
	d.fc.Del(key)
	return nil
}

//Features return supported features
func (d *Driver) Features() kvdb.Feature {
	return kvdb.FeatureTTLStore |
		kvdb.FeatureStore
}

func new() *Driver {
	return &Driver{}
}

type Config struct {
	Size int
}

func (c *Config) CreateDriver() (kvdb.Driver, error) {
	fc := freecache.NewCache(c.Size)
	d := new()
	d.fc = fc
	return d, nil
}

//Factory driver factory
func Factory(loader func(v interface{}) error) (kvdb.Driver, error) {
	c := &Config{}
	err := loader(c)
	if err != nil {
		return nil, err
	}
	return c.CreateDriver()
}

func init() {
	kvdb.Register("freecache", Factory)
}
