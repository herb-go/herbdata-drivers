package badgerdb

import (
	"bytes"
	"errors"
	"log"
	"time"

	badger "github.com/dgraph-io/badger"

	"github.com/herb-go/herbdata"
	"github.com/herb-go/herbdata/kvdb"
	"github.com/syndtr/goleveldb/leveldb"
)

const Features = kvdb.FeatureStore |
	kvdb.FeatureTTLStore |
	kvdb.FeaturePersistent |
	kvdb.FeatureNext |
	kvdb.FeaturePrev |
	kvdb.FeatureEmbedded

func convertError(err error) error {
	if err == nil {
		return err
	}
	if err == badger.ErrKeyNotFound {
		return herbdata.ErrNotFound
	}
	return err
}
func defaultErrHandler(err error) {
	log.Println(err)
}

type Driver struct {
	kvdb.Nop
	GCInterval time.Duration
	Database   string
	DB         *badger.DB
	Ticker     *time.Ticker
	GCLevel    float64
	ErrHandler func(error)
}

func (d *Driver) SetErrorHanlder(f func(error)) {
	d.ErrHandler = f
}

//Start start database
func (d *Driver) Start() error {
	var err error
	d.DB, err = badger.Open(badger.DefaultOptions(d.Database))
	if err != nil {
		return err
	}
	d.Ticker = time.NewTicker(d.GCInterval)
	go d.startGC()
	return nil
}
func (d *Driver) startGC() {
	for {
		for range d.Ticker.C {
		again:
			err := d.DB.RunValueLogGC(d.GCLevel)
			if err == nil {
				goto again
			}
			if err != badger.ErrNoRewrite {
				d.ErrHandler(err)
			}
			break
		}
	}

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
	d.Ticker.Stop()
	return nil
}

//Set set value by given key
func (d *Driver) Set(key []byte, value []byte) error {
	err := d.DB.Update(func(txn *badger.Txn) error {
		return txn.Set(key, value)
	})
	return err
}

//SetWithTTL set value by given key and ttl in second
func (d *Driver) SetWithTTL(key []byte, value []byte, ttlInSecond int64) error {
	if ttlInSecond <= 0 {
		return herbdata.ErrInvalidatedTTL
	}
	err := d.DB.Update(func(txn *badger.Txn) error {
		e := badger.NewEntry(key, value).WithTTL(time.Duration(ttlInSecond) * time.Second)
		err := txn.SetEntry(e)
		return err
	})
	return err
}

//Get get value by given key
func (d *Driver) Get(key []byte) ([]byte, error) {
	var value []byte
	var err error
	err = d.DB.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		err = item.Value(func(val []byte) error {
			value = append([]byte{}, val...)
			return nil
		})
		return err
	})
	if err != nil {
		return nil, convertError(err)
	}
	return value, nil
}

//Delete delete value by given key
func (d *Driver) Delete(key []byte) error {
	err := d.DB.Update(func(txn *badger.Txn) error {
		err := txn.Delete(key)
		return err
	})
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

	err = d.DB.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = limit + 1
		it := txn.NewIterator(opts)
		defer it.Close()
		var found bool
		if len(iter) == 0 {
			found = true
		}

		for it.Seek(iter); it.Valid(); it.Next() {
			item := it.Item()
			k := item.Key()
			if !found {
				if bytes.Compare(iter, k) >= 0 {
					continue
				}
				found = true
			}
			err := item.Value(func(v []byte) error {
				kv := &herbdata.KeyValue{
					Key:   k,
					Value: v,
				}
				result = append(result, kv.Clone())
				return nil
			})
			if err != nil {
				return err
			}
			if limit >= 0 && len(result) >= limit {
				newiter = append([]byte{}, k...)
				return nil
			}
		}
		return err
	})
	if err != nil {
		return nil, nil, err
	}
	if len(newiter) > 0 {
		return result, newiter, nil
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

	err = d.DB.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = limit + 1
		opts.Reverse = true
		it := txn.NewIterator(opts)
		defer it.Close()
		var found bool
		if len(iter) == 0 {
			found = true
		}

		for it.Seek(iter); it.Valid(); it.Next() {
			item := it.Item()
			k := item.Key()
			if !found {
				if bytes.Compare(iter, k) <= 0 {
					continue
				}
				found = true
			}
			err := item.Value(func(v []byte) error {
				kv := &herbdata.KeyValue{
					Key:   k,
					Value: v,
				}
				result = append(result, kv.Clone())
				return nil
			})
			if err != nil {
				return err
			}
			if limit >= 0 && len(result) >= limit {
				newiter = append([]byte{}, k...)
				return nil
			}
		}
		return err
	})
	if err != nil {
		return nil, nil, err
	}
	if len(newiter) > 0 {
		return result, newiter, nil
	}
	return result, nil, nil

}

//Features return supported features
func (d *Driver) Features() kvdb.Feature {
	return Features
}

//NewDriver create new driver
func NewDriver() *Driver {
	return &Driver{
		GCLevel:    0.5,
		GCInterval: 5 * time.Minute,
		ErrHandler: defaultErrHandler,
	}
}

type Config struct {
	Database           string
	GCIntervalDuration string
}

func (c *Config) ApplyTo(d *Driver) error {
	d.Database = c.Database
	return nil
}
func (c *Config) CreateDriver() (kvdb.Driver, error) {
	d := NewDriver()
	err := c.ApplyTo(d)
	if err != nil {
		return nil, err
	}
	if c.GCIntervalDuration != "" {
		dur, err := time.ParseDuration(c.GCIntervalDuration)
		if err != nil {
			return nil, err
		}
		if dur > 0 {
			d.GCInterval = dur
		}
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
	if c.Database == "" {
		return nil, errors.New("badgerdb: database path required")
	}
	return c.CreateDriver()
}

func init() {
	kvdb.Register("badgerdb", Factory)
}
