package bitcask_go

import (
	"os"
)

type IndexType = int8

const (
	BTree IndexType = iota + 1 // Btree索引

	ART // Adaptive Radix Tree索引

	// BPlusTree B+ 树索引，将索引存储到磁盘上
	BPlusTree
)

//Remark : 配置文件

type Options struct {
	// KV存储数据库 数据存储目录
	DirPath string
	// 单个存储文件可以存储的容量
	DataFileSize int64
	// 用户决定是否在写入数据是是否持久化
	SyncWrites bool
	// 数据库索引使用那种实现方式
	IndexType IndexType
	// 累计写到多少字节后进行持久化
	BytesPerSync uint
}

var DefaultOptions = Options{
	DirPath:      os.TempDir(),
	DataFileSize: 256 * 1024 * 1024,
	SyncWrites:   false,
	IndexType:    BTree, // ART  BPlusTree
}

// IteratorOptions 迭代器自定义配置
type IteratorOptions struct {
	// 遍历前缀为制定值的key，默认为空
	Prefix []byte
	// 是否反向遍历，默认false 正序
	Reverse bool
}

var DefaultIteratorOptions = IteratorOptions{
	Prefix:  nil,
	Reverse: false,
}

// WriteBatchOptions 批量写配置项
type WriteBatchOptions struct {
	// 一个批次当中最大的数据量
	MaxBatchNum uint

	// 提交时是否 sync 持久化
	SyncWrites bool
}

var DefaultWriteBatchOptions = WriteBatchOptions{
	MaxBatchNum: 10000,
	SyncWrites:  true,
}
