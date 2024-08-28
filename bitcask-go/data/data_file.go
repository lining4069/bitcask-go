package data

import "bitcask-go/fio"

// Remark : 数据文件抽象

// 数据文件默认后缀
const DataFileNameSuffix string = ".data"

// DataFile 数据文件
type DataFile struct {
	FileId    uint32
	WriteOff  int64
	IoManager fio.IOManager
}

// OpenDataFile 打开新的数据文件
func OpenDataFile(dirPath string, fileId uint32) (*DataFile, error) {
	return nil, nil
}

// Sync 文件持久化方法
func (df *DataFile) Sync() error {
	return nil
}

// 写数据
func (df *DataFile) Write(buf []byte) error {
	return nil
}

func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	return nil, 0, nil
}
