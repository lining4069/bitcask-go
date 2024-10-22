package bitcask_go

import (
	"bitcask-go/index"
	"os"
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
	IndexType index.IndexerType
}

var DefaultOptions = Options{
	DirPath:      os.TempDir(),
	DataFileSize: 256 * 1024 * 1024,
	SyncWrites:   false,
	IndexType:    index.Btree,
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
