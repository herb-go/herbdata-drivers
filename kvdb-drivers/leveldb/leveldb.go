package leveldb

import (
	"bytes"

	"github.com/herb-go/herbdata"
	"github.com/herb-go/herbdata/kvdb"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

const Features = kvdb.FeatureStore |
	kvdb.FeaturePersistent |
	kvdb.FeatureNext |
	kvdb.FeaturePrev |
	kvdb.FeatureEmbedded

func convertError(err error) error {
	if err == nil {
		return err
	}
	if err == leveldb.ErrNotFound {
		return herbdata.ErrNotFound
	}
	return err
}

type Driver struct {
	kvdb.Nop
	Database string
	DB       *leveldb.DB
}

//Start start database
func (d *Driver) Start() error {
	var err error
	d.DB, err = leveldb.OpenFile(d.Database, nil)
	return err
}

//Stop stop database
func (d *Driver) Stop() error {
	if d.DB != nil {
		err := d.DB.Close()
		if err == leveldb.ErrClosed {
			return nil
		}
		return err
	}
	return nil
}

//Set set value by given key
func (d *Driver) Set(key []byte, value []byte) error {
	return d.DB.Put(key, value, nil)
}

//Get get value by given key
func (d *Driver) Get(key []byte) ([]byte, error) {
	bs, err := d.DB.Get(key, nil)
	if err != nil {
		return nil, convertError(err)
	}
	return bs, nil
}

//Delete delete value by given key
func (d *Driver) Delete(key []byte) error {
	err := d.DB.Delete(key, nil)
	if err == leveldb.ErrNotFound {
		return nil
	}
	return err
}

//Next return keys after iter not more than given limit
//Empty iter (nil or 0 length []byte) will start a new search
//Return keyvalue ,newiter and any error if raised.
//Empty iter (nil or 0 length []byte) will be returned if no more keys
func (d *Driver) Next(iter []byte, limit int) (result []*herbdata.KeyValue, newiter []byte, err error) {
	if limit <= 0 {
		return nil, nil, kvdb.ErrUnsupportedNextLimit
	}
	iterrange := &util.Range{}
	if len(iter) > 0 {
		iterrange.Start = iter
	}
	it := d.DB.NewIterator(iterrange, nil)

	defer it.Release()
	var found bool
	if len(iter) == 0 {
		found = true
	}
	if it.First() {
		for {
			if !found {
				if bytes.Compare(iter, it.Key()) >= 0 {
					if !it.Next() {
						break
					}
					continue
				}
				found = true
			}
			kv := &herbdata.KeyValue{
				Key:   it.Key(),
				Value: it.Value(),
			}
			result = append(result, kv.Clone())
			if limit >= 0 && len(result) >= limit {
				return result, it.Key(), nil
			}
			if !it.Next() {
				break
			}
		}
	}
	err = it.Error()
	if err != nil {
		return nil, nil, err
	}
	return result, nil, nil
}

//Prev return keys before iter not more than given limit
//Empty iter (nil or 0 length []byte) will start a new search
//Return keys ,newiter and any error if raised.
//Empty iter (nil or 0 length []byte) will be returned if no more keys
func (d *Driver) Prev(iter []byte, limit int) (result []*herbdata.KeyValue, newiter []byte, err error) {
	if limit <= 0 {
		return nil, nil, kvdb.ErrUnsupportedNextLimit
	}
	iterrange := &util.Range{}
	if len(iter) > 0 {
		iterrange.Limit = iter
	}
	it := d.DB.NewIterator(iterrange, nil)
	defer it.Release()
	var found bool
	if len(iter) == 0 {
		found = true
	}
	if it.Last() {
		for {
			if !found {
				if bytes.Compare(iter, it.Key()) <= 0 {
					if !it.Prev() {
						break
					}
					continue
				}
				found = true
			}
			kv := &herbdata.KeyValue{
				Key:   it.Key(),
				Value: it.Value(),
			}
			result = append(result, kv.Clone())
			if limit >= 0 && len(result) >= limit {
				return result, it.Key(), nil
			}
			if !it.Prev() {
				break
			}
		}
	}
	err = it.Error()
	if err != nil {
		return nil, nil, err
	}
	return result, nil, nil
}

//Features return supported features
func (d *Driver) Features() kvdb.Feature {
	return Features
}

type Config struct {
	Database string
}

func (c *Config) ApplyTo(d *Driver) error {
	d.Database = c.Database
	return nil
}
func (c *Config) CreateDriver() (kvdb.Driver, error) {
	d := &Driver{}
	err := c.ApplyTo(d)
	if err != nil {
		return nil, err
	}
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
	kvdb.Register("leveldb", Factory)
}
