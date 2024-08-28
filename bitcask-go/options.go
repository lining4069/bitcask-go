package bitcask_go

import "bitcask-go/index"

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
