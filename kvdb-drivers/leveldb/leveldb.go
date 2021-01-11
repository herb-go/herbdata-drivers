package leveldb

import (
	"github.com/herb-go/herbdata"
	"github.com/herb-go/herbdata/kvdb"
	"github.com/syndtr/goleveldb/leveldb"
)

const Features = kvdb.FeatureStore |
	kvdb.FeatureNext |
	kvdb.FeaturePrev

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
		return d.DB.Close()
	}
	return nil
}

//Set set value by given key
func (d *Driver) Set(key []byte, value []byte) error {
	return d.DB.Put(key, value, nil)
}

//Get get value by given key
func (d *Driver) Get(key []byte) ([]byte, error) {
	return d.DB.Get(key, nil)
}

//Delete delete value by given key
func (d *Driver) Delete(key []byte) error {
	return d.DB.Delete(key, nil)
}

//Next return keys after iter not more than given limit
//Empty iter (nil or 0 length []byte) will start a new search
//Return keyvalue ,newiter and any error if raised.
//Empty iter (nil or 0 length []byte) will be returned if no more keys
func (d *Driver) Next(iter []byte, limit int) (kv []herbdata.KeyValue, newiter []byte, err error) {

}

//Prev return keys before iter not more than given limit
//Empty iter (nil or 0 length []byte) will start a new search
//Return keys ,newiter and any error if raised.
//Empty iter (nil or 0 length []byte) will be returned if no more keys
func (d *Driver) Prev(iter []byte, limit int) (kv []herbdata.KeyValue, newiter []byte, err error) {

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
