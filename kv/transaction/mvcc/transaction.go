package mvcc

import (
	"encoding/binary"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/codec"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/tsoutil"
)

// KeyError is a wrapper type so we can implement the `error` interface.
type KeyError struct {
	kvrpcpb.KeyError
}

func (ke *KeyError) Error() string {
	return ke.String()
}

// MvccTxn groups together writes as part of a single transaction. It also provides an abstraction over low-level
// storage, lowering the concepts of timestamps, writes, and locks into plain keys and values.
type MvccTxn struct {
	StartTS uint64
	Reader  storage.StorageReader
	writes  []storage.Modify
}

func NewMvccTxn(reader storage.StorageReader, startTs uint64) *MvccTxn {
	return &MvccTxn{
		Reader:  reader,
		StartTS: startTs,
	}
}

// Writes returns all changes added to this transaction.
func (txn *MvccTxn) Writes() []storage.Modify {
	return txn.writes
}

// PutWrite records a write at key and ts.
func (txn *MvccTxn) PutWrite(key []byte, ts uint64, write *Write) {
	// Your Code Here (4A).
	//startTS := write.StartTS
	//// get lock
	//lockValue, err := txn.Reader.GetCF(engine_util.CfLock, key)
	//if err != nil {
	//	return
	//}
	//// parse lock
	//lock, err := ParseLock(lockValue)
	//if err != nil {
	//	return
	//}
	//if lock.Ts == startTS {
	//	// delete lock
	//	txn.writes = append(txn.writes, storage.Modify{Data: storage.Delete{Key: key, Cf: engine_util.CfLock}})
	//	encodeKey := EncodeKey(key, ts)
	//	txn.writes = append(txn.writes, storage.Modify{Data: storage.Put{Cf: engine_util.CfWrite, Key: encodeKey, Value: write.ToBytes()}})
	//}
	encodeKey := EncodeKey(key, ts)
	txn.writes = append(txn.writes, storage.Modify{Data: storage.Put{Cf: engine_util.CfWrite, Key: encodeKey, Value: write.ToBytes()}})
}

// GetLock returns a lock if key is locked. It will return (nil, nil) if there is no lock on key, and (nil, err)
// if an error occurs during lookup.
func (txn *MvccTxn) GetLock(key []byte) (*Lock, error) {
	// Your Code Here (4A).
	value, err := txn.Reader.GetCF(engine_util.CfLock, key)
	if err != nil {
		return nil, err
	}
	if len(value) == 0 {
		return nil, nil
	}
	lock, err := ParseLock(value)
	if err != nil {
		return nil, err
	}
	return lock, nil
}

// PutLock adds a key/lock to this transaction.
func (txn *MvccTxn) PutLock(key []byte, lock *Lock) {
	// Your Code Here (4A).
	value, err := txn.Reader.GetCF(engine_util.CfLock, key)
	if err != nil {
		return
	}
	if len(value) == 0 {
		txn.writes = append(txn.writes, storage.Modify{Data: storage.Put{Key: key, Value: lock.ToBytes(), Cf: engine_util.CfLock}})
	}
}

// DeleteLock adds a delete lock to this transaction.
func (txn *MvccTxn) DeleteLock(key []byte) {
	// Your Code Here (4A).
	_, err := txn.Reader.GetCF(engine_util.CfLock, key)
	if err != nil {
		return
	}
	txn.writes = append(txn.writes, storage.Modify{Data: storage.Delete{Key: key, Cf: engine_util.CfLock}})
}

// GetValue finds the value for key, valid at the start timestamp of this transaction.
// I.e., the most recent value committed before the start of this transaction.
func (txn *MvccTxn) GetValue(key []byte) ([]byte, error) {
	// Your Code Here (4A).
	iterator := txn.Reader.IterCF(engine_util.CfWrite)
	var startTs uint64
	var write Write
	for iterator.Valid() {
		item := iterator.Item()
		codedKey := item.Key()
		userKey := DecodeUserKey(codedKey)
		if equals(userKey, key) {
			// get commit ts
			ts := decodeTimestamp(codedKey)
			if ts <= txn.StartTS {
				value, err := item.Value()
				if err != nil {
					return nil, err
				}
				write, err := ParseWrite(value)
				if err != nil {
					return nil, err
				}
				startTs = write.StartTS
				break
			}
		}
		iterator.Next()
	}
	iterator.Close()
	if startTs == 0 {
		return nil, nil
	}
	if write.Kind == WriteKindDelete {
		return nil, nil
	}
	// get value
	encodeKey := EncodeKey(key, startTs)
	val, err := txn.Reader.GetCF(engine_util.CfDefault, encodeKey)
	if err != nil {
		return nil, err
	}
	return val, nil
}

func equals(bytes1, bytes2 []byte) bool {
	if len(bytes1) == len(bytes2) {
		for i, v := range bytes1 {
			if v != bytes2[i] {
				return false
			}
		}
		return true
	}
	return false
}

// PutValue adds a key/value write to this transaction.
func (txn *MvccTxn) PutValue(key []byte, value []byte) {
	// Your Code Here (4A).
	txn.writes = append(txn.writes, storage.Modify{Data: storage.Put{Cf: engine_util.CfDefault, Key: EncodeKey(key, txn.StartTS), Value: value}})
}

// DeleteValue removes a key/value pair in this transaction.
func (txn *MvccTxn) DeleteValue(key []byte) {
	// Your Code Here (4A).
	txn.writes = append(txn.writes, storage.Modify{Data: storage.Delete{Cf: engine_util.CfDefault, Key: EncodeKey(key, txn.StartTS)}})
}

// CurrentWrite searches for a write with this transaction's start timestamp. It returns a Write from the DB and that
// write's commit timestamp, or an error.
func (txn *MvccTxn) CurrentWrite(key []byte) (*Write, uint64, error) {
	// Your Code Here (4A).
	iterator := txn.Reader.IterCF(engine_util.CfWrite)
	var commitTs uint64
	var w *Write
	for iterator.Valid() {
		item := iterator.Item()
		codedKey := item.Key()
		userKey := DecodeUserKey(codedKey)
		if equals(userKey, key) {
			value, err := item.Value()
			if err != nil {
				return nil, 0, err
			}
			write, err := ParseWrite(value)
			if err != nil {
				return nil, 0, err
			}
			if write.StartTS == txn.StartTS {
				w = write
				commitTs = decodeTimestamp(codedKey)
				break
			}
			if write.StartTS < txn.StartTS {
				return nil, 0, nil
			}
		}
		iterator.Next()
	}
	if commitTs == 0 {
		return nil, 0, nil
	}
	return w, commitTs, nil
}

// MostRecentWrite finds the most recent write with the given key. It returns a Write from the DB and that
// write's commit timestamp, or an error.
func (txn *MvccTxn) MostRecentWrite(key []byte) (*Write, uint64, error) {
	// Your Code Here (4A).
	iterator := txn.Reader.IterCF(engine_util.CfWrite)
	for iterator.Valid() {
		item := iterator.Item()
		codedKey := item.Key()
		userKey := DecodeUserKey(codedKey)
		if equals(userKey, key) {
			value, err := item.Value()
			if err != nil {
				return nil, 0, err
			}
			write, err := ParseWrite(value)
			if err != nil {
				return nil, 0, err
			}
			commitTs := decodeTimestamp(codedKey)
			return write, commitTs, nil
		}
		iterator.Next()
	}
	return nil, 0, nil
}

// EncodeKey encodes a user key and appends an encoded timestamp to a key. Keys and timestamps are encoded so that
// timestamped keys are sorted first by key (ascending), then by timestamp (descending). The encoding is based on
// https://github.com/facebook/mysql-5.6/wiki/MyRocks-record-format#memcomparable-format.
func EncodeKey(key []byte, ts uint64) []byte {
	encodedKey := codec.EncodeBytes(key)
	newKey := append(encodedKey, make([]byte, 8)...)
	binary.BigEndian.PutUint64(newKey[len(encodedKey):], ^ts)
	return newKey
}

// DecodeUserKey takes a key + timestamp and returns the key part.
func DecodeUserKey(key []byte) []byte {
	_, userKey, err := codec.DecodeBytes(key)
	if err != nil {
		panic(err)
	}
	return userKey
}

// decodeTimestamp takes a key + timestamp and returns the timestamp part.
func decodeTimestamp(key []byte) uint64 {
	left, _, err := codec.DecodeBytes(key)
	if err != nil {
		panic(err)
	}
	return ^binary.BigEndian.Uint64(left)
}

// PhysicalTime returns the physical time part of the timestamp.
func PhysicalTime(ts uint64) uint64 {
	return ts >> tsoutil.PhysicalShiftBits
}
