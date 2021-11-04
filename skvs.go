//skvs - Simple key value store - is a package that provides a presistent key-value store for go values
//It stores a mapping of a string to any gob-encoded value.
//
//It provides a simple API - you can Insert(), Retrieve() or Delete() entries
//
//BoltDB is used for storage for ease and performance

package skvs

import (
	"bytes"
	"encoding/gob"
	"errors"
	"time"

	"github.com/boltdb/bolt"
)

//KVStore represents the key-value store. Since it has bolt.DB as a field, create and open
//a store using the Open() method and Close() it when done

type KVStore struct {
	db *bolt.DB
}

var (
	//Custom errors
	//ErrNotFound is returned when the key supplied to Retrieve() or Delete() methods
	//does not exist
	ErrNotFound = errors.New("skv: key not found")

	//ErrBadValue is returned when the key supplied to Insert() is nil
	ErrBadValue = errors.New("skv: bad value")

	bucketName = []byte("kv")
)

//Open() is to open a key-value store by input the database file path (file will be create if it doesn't exist)
//leading directions must already exist (they won't be automatically created)
//It is not a method function

func Open(path string) (*KVStore, error) {
	opts := &bolt.Options{
		Timeout: 50 * time.Millisecond,
	}
	if db, err := bolt.Open(path, 0640, opts); err != nil {
		return nil, err
	} else {
		err := db.Update(func(tx *bolt.Tx) error {
			_, err := tx.CreateBucketIfNotExists(bucketName)
			return err
		})
		if err != nil {
			return nil, err
		} else {
			return &KVStore{db: db}, nil
		}
	}
}

//Inserting into a store using the Insert() method
//Key can be empty,but the value cannot be nil, else the method will return
// ErrBadValue

func (kvs *KVStore) Insert(key string, value interface{}) error {
	if value == nil {
		return ErrBadValue
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(value); err != nil {
		return err
	}
	return kvs.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketName).Put([]byte(key), buf.Bytes())
	})
}

// Retrieving from the store using the Retrieve method. It returns a pointer to the value

func (kvs *KVStore) Retrieve(key string, value interface{}) error {
	return kvs.db.View(func(tx *bolt.Tx) error {
		cur := tx.Bucket(bucketName).Cursor()
		if k, v := cur.Seek([]byte(key)); k == nil || string(k) != key {
			return ErrNotFound
		} else if value == nil {
			return nil
		} else {
			data := gob.NewDecoder(bytes.NewReader(v))
			return data.Decode(value)
		}
	})
}

// Delete an entry in the store with the give Key. If no such key is present in the store,
// it returns ErrNotFound

func (kvs *KVStore) Delete(key string) error {
	return kvs.db.Update(func(tx *bolt.Tx) error {
		cur := tx.Bucket(bucketName).Cursor()
		if k, _ := cur.Seek([]byte(key)); k == nil || string(k) != key {
			return ErrNotFound
		} else {
			return cur.Delete()
		}
	})
}

// Close() closes the key-value store db file. Unlike Open(), it is a method

func (kvs *KVStore) Close() error {
	return kvs.db.Close()
}
