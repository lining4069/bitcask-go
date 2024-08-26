package fio

// remark: 磁盘io接口

const DataFilePerm = 0644

// IOManager 抽象出接口，可以扩展不同的磁盘存储操作的实现
type IOManager interface {
	// Read  从文件给定位置获取对应的数值
	Read([]byte, int64) (int, error)
	// Write 写入字节数据到文件
	Write([]byte) (int, error)
	//Sync 持久化数据
	Sync() error
	// Close 关闭文件
	Close() error
}
