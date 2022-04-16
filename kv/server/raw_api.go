package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	resp := &kvrpcpb.RawGetResponse{}
	//1.获取reader
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return resp, err
	}
	//2.读取数据
	val, err := reader.GetCF(req.Cf, req.Key)
	if val == nil {
		resp.NotFound = true
		return resp, nil
	}
	resp.Value = val
	return resp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	//1.构造put请求的modify
	modify := storage.Modify{Data: storage.Put{Key: req.Key, Value: req.Value, Cf: req.Cf}}
	//2.写入
	modifies := []storage.Modify{modify}
	err := server.storage.Write(req.Context, modifies)
	if err != nil {
		return nil, err
	}
	//3.构造响应
	resp := &kvrpcpb.RawPutResponse{}
	return resp, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	resp := &kvrpcpb.RawDeleteResponse{}
	//1.构造delete请求的modify
	modify := storage.Modify{Data: storage.Delete{Key: req.Key, Cf: req.Cf}}
	//2.写入该操作
	modifies := []storage.Modify{modify}
	err := server.storage.Write(req.Context, modifies)
	if err != nil {
		return resp, nil
	}
	//3.构造响应
	return resp, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	//1.获取reader
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	//2.获取该cf的迭代器
	it := reader.IterCF(req.Cf)
	defer it.Close()
	kvPairs := make([]*kvrpcpb.KvPair, 0)
	//3.找到迭代起始位置
	it.Seek(req.StartKey)
	//4.迭代
	for it.Seek(req.StartKey); len(kvPairs) < int(req.Limit) && it.Valid(); it.Next() {
		item := it.Item()
		pair := &kvrpcpb.KvPair{}
		//fmt.Printf("item:%v\n", item)
		pair.Key = item.KeyCopy(nil)
		pair.Value, err = item.ValueCopy(nil)
		if err != nil {
			return nil, err
		}
		kvPairs = append(kvPairs, pair)
		//fmt.Printf("key:%v; value:%v\n", pair.Key, pair.Value)
	}
	resp := &kvrpcpb.RawScanResponse{}
	resp.Kvs = kvPairs
	return resp, nil
}
