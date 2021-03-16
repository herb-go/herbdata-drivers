package boltdb

import (
	"bytes"
	"errors"
	"os"

	bolt "github.com/etcd-io/bbolt"
	"github.com/herb-go/herbdata"
	"github.com/herb-go/herbdata/kvdb"
)

const Features = kvdb.FeatureStore |
	kvdb.FeatureNext |
	kvdb.FeaturePrev |
	kvdb.FeatureEmbedded

type Driver struct {
	kvdb.Nop
	Database string
	FileMode os.FileMode
	Bucket   []byte
	DB       *bolt.DB
}

//Start start database
func (d *Driver) Start() error {
	var err error
	d.DB, err = bolt.Open(d.Database, d.FileMode, nil)
	if err != nil {
		return err
	}
	return d.DB.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(d.Bucket)
		return err
	})

}

//Stop stop database
func (d *Driver) Stop() error {
	if d.DB != nil {
		err := d.DB.Close()
		if err == bolt.ErrDatabaseNotOpen {
			return nil
		}
		return err
	}
	return nil
}

//Set set value by given key
func (d *Driver) Set(key []byte, value []byte) error {
	return d.DB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(d.Bucket)
		return b.Put(key, value)
	})
}

//Get get value by given key
func (d *Driver) Get(key []byte) ([]byte, error) {
	var result []byte
	var found bool
	err := d.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(d.Bucket)
		data := b.Get(key)
		if data != nil {
			result = append([]byte{}, data...)
			found = true
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, herbdata.ErrNotFound
	}
	return result, nil
}

//Delete delete value by given key
func (d *Driver) Delete(key []byte) error {
	return d.DB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(d.Bucket)
		return b.Delete(key)
	})
}

//Next return keys after iter not more than given limit
//Empty iter (nil or 0 length []byte) will start a new search
//Return keyvalue ,newiter and any error if raised.
//Empty iter (nil or 0 length []byte) will be returned if no more keys
func (d *Driver) Next(iter []byte, limit int) (result []*herbdata.KeyValue, newiter []byte, err error) {
	if limit <= 0 {
		return nil, nil, kvdb.ErrUnsupportedNextLimit
	}
	var found bool
	if len(iter) == 0 {
		found = true
	}
	err = d.DB.View(func(tx *bolt.Tx) error {
		var k, v []byte
		b := tx.Bucket(d.Bucket)
		c := b.Cursor()
		if len(iter) > 0 {
			k, v = c.Seek(iter)

		} else {
			k, v = c.First()
		}
		for k != nil {
			if !found {
				if bytes.Compare(iter, k) >= 0 {
					k, v = c.Next()
					continue
				}
				found = true
			}
			kv := &herbdata.KeyValue{
				Key:   k,
				Value: v,
			}
			result = append(result, kv.Clone())
			if limit >= 0 && len(result) >= limit {
				newiter = append([]byte{}, k...)
				return nil
			}
			k, v = c.Next()
		}
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	return result, newiter, nil
}

//Prev return keys before iter not more than given limit
//Empty iter (nil or 0 length []byte) will start a new search
//Return keys ,newiter and any error if raised.
//Empty iter (nil or 0 length []byte) will be returned if no more keys
func (d *Driver) Prev(iter []byte, limit int) (result []*herbdata.KeyValue, newiter []byte, err error) {
	if limit <= 0 {
		return nil, nil, kvdb.ErrUnsupportedNextLimit
	}
	var found bool
	if len(iter) == 0 {
		found = true
	}

	err = d.DB.View(func(tx *bolt.Tx) error {
		var k, v []byte
		b := tx.Bucket(d.Bucket)
		c := b.Cursor()
		if len(iter) > 0 {
			k, v = c.Seek(iter)
			if bytes.Compare(k, iter) != 0 {
				k, v = c.Prev()
			}
		} else {
			k, v = c.Last()
		}
		for k != nil {
			if !found {
				if bytes.Compare(iter, k) <= 0 {
					k, v = c.Prev()
					continue
				}
				found = true
			}

			kv := &herbdata.KeyValue{
				Key:   k,
				Value: v,
			}
			result = append(result, kv.Clone())
			if limit >= 0 && len(result) >= limit {
				newiter = append([]byte{}, k...)
				return nil
			}
			k, v = c.Prev()
		}
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	return result, newiter, nil
}

//Features return supported features
func (d *Driver) Features() kvdb.Feature {
	return Features
}

type Config struct {
	Database string
	Bucket   string
}

func (c *Config) ApplyTo(d *Driver) error {
	if c.Database == "" {
		return errors.New("boltdb: database path required")
	}
	if c.Bucket == "" {
		return errors.New("boltdb: bucket required")
	}
	d.Database = c.Database
	d.Bucket = []byte(c.Bucket)
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
	kvdb.Register("boltdb", Factory)
}
