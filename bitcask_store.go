package raftbitcask

import (
	"bytes"
	"errors"
	"sort"

	"github.com/hashicorp/raft"
	gobitcask "github.com/ldmtam/go-bitcask"
)

var (
	// Prefix names to distingish between logs and conf
	prefixLogs = []byte{0x0}
	prefixConf = []byte{0x1}

	// ErrKeyNotFound is an error indicating a given key does not exist
	ErrKeyNotFound = errors.New("not found")
)

// BitcaskStore provides access to Bitcask for Raft to store and retrieve
// log entries. It also provides key/value storage, and can be used as
// a LogStore and StableStore.
type BitcaskStore struct {
	db *gobitcask.Bitcask
}

// New uses the supplied options to open the Bitcask db and prepare it for
// use as a raft backend.
func New(opts ...gobitcask.OptFn) (*BitcaskStore, error) {
	db, err := gobitcask.New(opts...)
	if err != nil {
		return nil, err
	}

	return &BitcaskStore{
		db: db,
	}, nil
}

// Close is used to gracefully close the DB connection.
func (b *BitcaskStore) Close() error {
	return b.db.Close()
}

// FirstIndex returns the first known index from the Raft log.
func (b *BitcaskStore) FirstIndex() (uint64, error) {
	logKeys := b.getLogKeys()
	if len(logKeys) == 0 {
		return 0, nil
	}

	sort.Slice(logKeys, func(i, j int) bool {
		return bytesToUint64(logKeys[i][1:]) < bytesToUint64(logKeys[j][1:])
	})

	return bytesToUint64(logKeys[0][1:]), nil
}

// LastIndex returns the last known index from the Raft log.
func (b *BitcaskStore) LastIndex() (uint64, error) {
	logKeys := b.getLogKeys()
	if len(logKeys) == 0 {
		return 0, nil
	}

	sort.Slice(logKeys, func(i, j int) bool {
		return bytesToUint64(logKeys[i][1:]) > bytesToUint64(logKeys[j][1:])
	})

	return bytesToUint64(logKeys[0][1:]), nil
}

// GetLog gets a log entry from Badger at a given index.
func (b *BitcaskStore) GetLog(index uint64, log *raft.Log) error {
	key := append(prefixLogs, uint64ToBytes(index)...)
	val, err := b.db.Get(key)
	if err == gobitcask.ErrKeyNotFound {
		return raft.ErrLogNotFound
	}
	if err != nil {
		return err
	}

	return decodeMsgPack(val, log)
}

// StoreLog stores a single raft log.
func (b *BitcaskStore) StoreLog(log *raft.Log) error {
	valBuffer, err := encodeMsgPack(log)
	if err != nil {
		return err
	}

	key := append(prefixLogs, uint64ToBytes(log.Index)...)

	return b.db.Put(key, valBuffer.Bytes())
}

// StoreLogs stores a set of raft logs.
func (b *BitcaskStore) StoreLogs(logs []*raft.Log) error {
	for _, log := range logs {
		err := b.StoreLog(log)
		if err != nil {
			return err
		}
	}

	return nil
}

// DeleteRange deletes logs within a given range inclusively.
func (b *BitcaskStore) DeleteRange(min, max uint64) error {
	for i := min; i <= max; i++ {
		key := append(prefixLogs, uint64ToBytes(i)...)
		err := b.db.Delete(key)
		if err == gobitcask.ErrKeyNotFound {
			continue
		}
		if err != nil {
			return err
		}
	}

	return nil
}

// Set is used to set a key/value set outside of the raft log.
func (b *BitcaskStore) Set(key []byte, val []byte) error {
	confKey := append(prefixConf, key...)
	return b.db.Put(confKey, val)
}

// Get is used to retrieve a value from the k/v store by key
func (b *BitcaskStore) Get(key []byte) ([]byte, error) {
	confKey := append(prefixConf, key...)

	val, err := b.db.Get(confKey)
	if err == gobitcask.ErrKeyNotFound {
		return nil, ErrKeyNotFound
	}
	if err != nil {
		return nil, err
	}

	return val, nil
}

// SetUint64 is like Set, but handles uint64 values
func (b *BitcaskStore) SetUint64(key []byte, val uint64) error {
	confKey := append(prefixConf, key...)
	return b.Set(confKey, uint64ToBytes(val))
}

// GetUint64 is like Get, but handles uint64 values
func (b *BitcaskStore) GetUint64(key []byte) (uint64, error) {
	confKey := append(prefixConf, key...)

	val, err := b.Get(confKey)
	if err != nil {
		return 0, err
	}
	return bytesToUint64(val), nil
}

func (b *BitcaskStore) getLogKeys() [][]byte {
	var logKeys [][]byte

	allKeys := b.db.ListKeys()
	for _, key := range allKeys {
		if !bytes.HasPrefix(key, prefixLogs) {
			continue
		}
		logKeys = append(logKeys, key)
	}

	return logKeys
}
