package redisdb

import (
	"github.com/gomodule/redigo/redis"
	"github.com/herb-go/datasource/redis/redispool"
	"github.com/herb-go/herbdata"
	"github.com/herb-go/herbdata/kvdb"
)

type Driver struct {
	kvdb.Nop
	Pool    *redis.Pool
	NoMulti bool
	Prefix  string
}

//Features return supported features
func (d *Driver) Features() kvdb.Feature {
	return kvdb.FeatureStore |
		kvdb.FeatureInsert |
		kvdb.FeatureUpdate |
		kvdb.FeatureCounter |
		kvdb.FeatureTTLStore |
		kvdb.FeatureTTLInsert |
		kvdb.FeatureTTLUpdate |
		kvdb.FeatureTTLCounter |
		kvdb.FeaturePersistent |
		kvdb.FeatureStable
}

// Close close database
func (d *Driver) Close() error {
	return d.Pool.Close()
}
func (d *Driver) getKey(key []byte) string {
	return d.Prefix + string(kvdb.SuggestedDataPrefix) + string(key)
}
func (d *Driver) getCounterKey(key []byte) string {
	return d.Prefix + string(kvdb.SuggestedCounterPrefix) + string(key)
}

//Set set value by given key
func (d *Driver) Set(key []byte, value []byte) error {
	conn := d.Pool.Get()
	defer conn.Close()
	_, err := conn.Do("SET", d.getKey(key), value)
	return convertError(err)
}

//Get get value by given key
func (d *Driver) Get(key []byte) ([]byte, error) {
	conn := d.Pool.Get()
	defer conn.Close()
	data, err := redis.Bytes(conn.Do("GET", d.getKey(key)))
	if err != nil {
		return nil, convertError(err)
	}
	return data, nil
}

//Delete delete value by given key
func (d *Driver) Delete(key []byte) error {
	conn := d.Pool.Get()
	defer conn.Close()
	_, err := conn.Do("DEL", d.getKey(key))
	return convertError(err)
}

//SetWithTTL set value by given key and ttl in second
func (d *Driver) SetWithTTL(key []byte, value []byte, ttlInSecond int64) error {
	if ttlInSecond < 0 {
		return herbdata.ErrInvalidatedTTL
	}
	conn := d.Pool.Get()
	defer conn.Close()
	_, err := conn.Do("SET", d.getKey(key), value, "EX", ttlInSecond)
	return convertError(err)
}

//SetCounter set counter value with given key
func (d *Driver) SetCounter(key []byte, value int64) error {
	conn := d.Pool.Get()
	defer conn.Close()
	_, err := conn.Do("SET", d.getCounterKey(key), value)
	return convertError(err)
}

//IncreaseCounter increace counter value with given key and increasement.
//Value not existed coutn as 0.
//Return final value and any error if raised.
func (d *Driver) IncreaseCounter(key []byte, incr int64) (int64, error) {
	conn := d.Pool.Get()
	defer conn.Close()
	data, err := redis.Int64(conn.Do("INCRBY", d.getCounterKey(key), incr))
	return data, convertError(err)
}

//IncreaseCounterWithTTL increace counter value with given key ,increasement,and ttl in second
//Value not existed coutn as 0.
//Return final value and any error if raised.
func (d *Driver) IncreaseCounterWithTTL(key []byte, incr int64, ttlInSecond int64) (int64, error) {
	if d.NoMulti {
		return d.increaseCounterWithTTLNoMulti(key, incr, ttlInSecond)
	}
	var err error
	if ttlInSecond < 0 {
		return 0, herbdata.ErrInvalidatedTTL
	}
	conn := d.Pool.Get()
	defer conn.Close()
	_, err = conn.Do("Multi")
	if err != nil {
		return 0, err
	}
	err = conn.Send("INCRBY", d.getCounterKey(key), incr)
	err = convertError(err)
	if err != nil {
		return 0, err
	}
	err = conn.Send("EXPIRE", d.getCounterKey(key), ttlInSecond)
	err = convertError(err)
	if err != nil {
		return 0, err
	}
	var data int64
	result, err := redis.Values(conn.Do("Exec"))
	if err != nil {
		return 0, err
	}
	_, err = redis.Scan(result, &data)
	if err != nil {
		return 0, err
	}

	return data, nil
}

func (d *Driver) increaseCounterWithTTLNoMulti(key []byte, incr int64, ttlInSecond int64) (int64, error) {
	if ttlInSecond < 0 {
		return 0, herbdata.ErrInvalidatedTTL
	}
	conn := d.Pool.Get()
	defer conn.Close()

	data, err := redis.Int64(conn.Do("INCRBY", d.getCounterKey(key), incr))
	err = convertError(err)
	if err != nil {
		return 0, err
	}
	_, err = conn.Do("EXPIRE", d.getCounterKey(key), ttlInSecond)
	err = convertError(err)
	if err != nil {
		return 0, err
	}
	return data, nil

}

//SetCounterWithTTL set counter value with given key and ttl in second
func (d *Driver) SetCounterWithTTL(key []byte, value int64, ttlInSecond int64) error {
	if ttlInSecond < 0 {
		return herbdata.ErrInvalidatedTTL
	}
	conn := d.Pool.Get()
	defer conn.Close()
	_, err := conn.Do("SET", d.getCounterKey(key), value, "EX", ttlInSecond)

	return convertError(err)
}

//GetCounter get counter value with given key
//Value not existed coutn as 0.
func (d *Driver) GetCounter(key []byte) (int64, error) {
	conn := d.Pool.Get()
	defer conn.Close()
	data, err := redis.Int64(conn.Do("GET", d.getCounterKey(key)))
	err = convertError(err)
	if err == herbdata.ErrNotFound {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return data, nil
}

//DeleteCounter delete counter value with given key
func (d *Driver) DeleteCounter(key []byte) error {
	conn := d.Pool.Get()
	defer conn.Close()
	_, err := conn.Do("DEL", d.getCounterKey(key))
	return convertError(err)
}

//InsertWithTTL insert value with given key and ttl in second.
//Insert will fail if data with given key exists.
//Return if operation success and any error if raised
func (d *Driver) InsertWithTTL(key []byte, value []byte, ttlInSecond int64) (bool, error) {
	if ttlInSecond < 0 {
		return false, herbdata.ErrInvalidatedTTL
	}
	conn := d.Pool.Get()
	defer conn.Close()
	_, err := redis.String(conn.Do("SET", d.getKey(key), value, "EX", ttlInSecond, "NX"))
	err = convertError(err)
	if err == herbdata.ErrNotFound {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil

}

//Update update value with given key.
//Update will fail if data with given key does nto exist.
//Return if operation success and any error if raised
func (d *Driver) Update(key []byte, value []byte) (bool, error) {
	conn := d.Pool.Get()
	defer conn.Close()
	_, err := redis.String(conn.Do("SET", d.getKey(key), value, "XX"))
	err = convertError(err)
	if err == herbdata.ErrNotFound {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

//UpdateWithTTL update value with given key and ttl in second.
//Update will fail if data with given key does nto exist.
//Return if operation success and any error if raised
func (d *Driver) UpdateWithTTL(key []byte, value []byte, ttlInSecond int64) (bool, error) {
	if ttlInSecond < 0 {
		return false, herbdata.ErrInvalidatedTTL
	}
	conn := d.Pool.Get()
	defer conn.Close()
	_, err := redis.String(conn.Do("SET", d.getKey(key), value, "EX", ttlInSecond, "XX"))
	err = convertError(err)
	if err == herbdata.ErrNotFound {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// Insert insert value with given key.
// Insert will fail if data with given key exists.
// Return if operation success and any error if raised
func (d *Driver) Insert(key []byte, value []byte) (bool, error) {
	conn := d.Pool.Get()
	defer conn.Close()
	_, err := redis.String(conn.Do("SET", d.getKey(key), value, "NX"))
	err = convertError(err)
	if err == herbdata.ErrNotFound {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func new() *Driver {
	return &Driver{}
}

type Config struct {
	redispool.Config
	Prefix  string
	NoMulti bool
}

func convertError(err error) error {
	if err == nil {
		return err
	}
	if err == redis.ErrNil {
		return herbdata.ErrNotFound
	}

	return err
}

func (c *Config) CreateDriver() (kvdb.Driver, error) {
	p := redispool.New()
	err := c.Config.ApplyTo(p)
	if err != nil {
		return nil, err
	}
	d := new()
	d.Prefix = c.Prefix
	d.NoMulti = c.NoMulti
	d.Pool = p.Open()
	return d, nil
}
