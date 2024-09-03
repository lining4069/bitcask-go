package index

// remark: 内存索引操作定义

import (
	"bitcask-go/data"
	"bytes"
	"github.com/google/btree"
)

// Indexer 通用内存索引接口
// 不同存储数据结构，通过实现Indexer接口实现扩展
type Indexer interface {
	// Put 内存索引中添加索引（索引，存储信息）
	Put(key []byte, pos *data.LogRecordPos) bool
	// Get 根据索引key，获取数据存储信息 LogRecordPos()
	Get(key []byte) *data.LogRecordPos
	// Delete 删除数据索引
	Delete(key []byte) bool
	// Iterator 获取索引迭代器
	Iterator(reverse bool) Iterator
	// Size 获取索引引擎中元素数量
	Size() int
}

// Iterator 通用索引迭代器
type Iterator interface {
	// Rewind 重新回到迭代器的起点，即第一个数据
	Rewind()

	// Seek 根据传入的 key 查找到第一个大于（或小于）等于的目标 key，根据从这个 key 开始遍历
	Seek(key []byte)

	// Next 跳转到下一个 key
	Next()

	// Valid 是否有效，即是否已经遍历完了所有的 key，用于退出遍历
	Valid() bool

	// Key 当前遍历位置的 Key 数据
	Key() []byte

	// Value 当前遍历位置的 Value 数据
	Value() *data.LogRecordPos

	// Close 关闭迭代器，释放相应资源
	Close()
}

// Item 使用google btree实现内存索引时，需要实现google btree Item数据结构 索引存储 操作单位
type Item struct {
	key []byte
	pos *data.LogRecordPos
}

func (ai *Item) Less(bi btree.Item) bool {
	return bytes.Compare(ai.key, bi.(*Item).key) == -1
}

// 索引引擎
type IndexerType int8

const (
	// B树索引
	Btree IndexerType = iota + 1
	// 自适应计数树索引
	ART
)

// NewIndexer 创建指定实现的内存索引引擎实例
func NewIndexer(typ IndexerType) Indexer {
	switch typ {
	case Btree:
		return NewBTree()
	case ART:
		return nil
	default:
		panic("unSupport indexer type")
	}
}
