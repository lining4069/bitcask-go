package index

// remark: 内存索引操作定义

import (
	"bitcask-go/data"
	"bytes"
	"github.com/google/btree"
)

// Indexer 内存通用索引接口
// 不同存储数据结构，通过实现Indexer接口实现扩展
type Indexer interface {
	// Put 内存索引中添加索引（索引，存储信息）
	Put(key []byte, pos *data.LogRecordPos) bool
	// Get 根据索引key，获取数据存储信息 LogRecordPos()
	Get(key []byte) *data.LogRecordPos
	// Delete 删除数据索引
	Delete(key []byte) bool
}

// Item 使用google btree实现内存索引时，需要实现google btree Item数据结构
type Item struct {
	key []byte
	pos *data.LogRecordPos
}

func (ai *Item) Less(bi btree.Item) bool {
	return bytes.Compare(ai.key, bi.(*Item).key) == -1
}
