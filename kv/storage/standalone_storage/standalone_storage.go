package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"sync"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	db *badger.DB
	m  map[string]engine_util.DBIterator
	mu sync.Mutex
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	//create a database with engine_util
	db := engine_util.CreateDB(conf.DBPath, conf.Raft)
	aloneStorage := StandAloneStorage{}
	aloneStorage.db = db
	aloneStorage.m = make(map[string]engine_util.DBIterator)
	return &aloneStorage
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	err := s.db.Close()
	if err != nil {
		return err
	}
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return s, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	for _, modify := range batch {
		switch modify.Data.(type) {
		case storage.Put:
			err := engine_util.PutCF(s.db, modify.Cf(), modify.Key(), modify.Value())
			if err != nil {
				return err
			}
		case storage.Delete:
			err := engine_util.DeleteCF(s.db, modify.Cf(), modify.Key())
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *StandAloneStorage) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCF(r.db, cf, key)
	if err != nil {
		return nil, nil
	}
	return val, nil
}

func (r *StandAloneStorage) IterCF(cf string) engine_util.DBIterator {
	r.mu.Lock()
	defer r.mu.Unlock()
	if it, ok := r.m[cf]; ok {
		return it
	}
	iterator := engine_util.NewCFIterator(cf, r.db.NewTransaction(false))
	r.m[cf] = iterator
	return iterator
}
func (r *StandAloneStorage) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, iterator := range r.m {
		iterator.Close()
	}
}
