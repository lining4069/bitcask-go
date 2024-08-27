package bitcask_go

import (
	"bitcask-go/data"
	"bitcask-go/index"
	"sync"
)

// Remark: KV存储模型

// DB 核心KV存储模型 包含两个核心方法 PUT和GET方法
type DB struct {
	// 读写锁
	mu *sync.RWMutex
	// 当前活跃数据文件 用于写入
	activateFile *data.DataFile
	// 旧文件
	orderFiles map[uint32]*data.DataFile
	// 配置
	options *Options
	// 内存索引引擎
	index index.Indexer
}

// Put 写入方法 key不能为空
func (db *DB) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrorKeyIsEmpty
	}
	// 构造KV模型数据存储实例
	logRecord := data.LogRecord{
		Key:   key,
		Value: value,
		Type:  data.LogRecordNormal,
	}
	// 存储到磁盘中，并返回内存索引信息
	pos, err := db.appendLogRecord(&logRecord)
	if err != nil {
		return err
	}
	// 将新增的数据加入内存索引
	if ok := db.index.Put(key, pos); !ok {
		return ErrIndexUpdateFailed
	}

	return nil
}

// Get 查询数据
func (db *DB) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, ErrorKeyIsEmpty
	}
	// 从内存索引信息中，获取数据的存储信息
	pos := db.index.Get(key)
	if pos == nil {
		return nil, ErrKeyNotFound
	}
	// 内存索引存在
	var dataFile *data.DataFile
	if db.activateFile.FileId == pos.Fid { // 要查询的数据文件为当前活跃文件
		dataFile = db.activateFile
	} else {
		dataFile = db.orderFiles[pos.Fid]
	}
	//存储文件不存在
	if dataFile == nil {
		return nil, ErrDataFileNotFound
	}
	// 根据偏移量读取数据构建logRecord
	logRecord, err := dataFile.ReadLogRecord(pos.Offset)
	if err != nil {
		return nil, err
	}
	// 处理当前查询数据为删除数据
	if logRecord.Type == data.LogRecordDeleted {
		return nil, ErrKeyNotFound
	}
	return logRecord.Value, err
}

// appendLogRecord 存储操作主函数
func (db *DB) appendLogRecord(dataRecord *data.LogRecord) (*data.LogRecordPos, error) {
	// 并发安全
	db.mu.Lock()
	defer db.mu.Unlock()
	// 存储 初始化 首次存储数据时，需要知道初始化，创建活跃文件
	if db.activateFile == nil {
		// 设置可用于写入和活跃文件
		if err := db.setActivateDataFile(); err != nil {
			return nil, err
		}
	}
	// 对写入数据编码获取长度
	encRecord, size := data.EncodeLogRecord(dataRecord)
	// 当前编码后数据长度大于剩余可存储容量
	// 处理：1、持久化当前活跃文件，2、打开新的活跃文件
	if db.activateFile.WriteOff+size > db.options.DataFileSize {
		// 持久化当前活跃文件
		if err := db.activateFile.Sync(); err != nil {
			return nil, err
		}
		// 将活跃文件转为旧文件
		db.orderFiles[db.activateFile.FileId] = db.activateFile
		// 打开新的活跃文件
		if err := db.setActivateDataFile(); err != nil {
			return nil, err
		}
	}
	// 获取到可用的获取文件
	// 写入数据
	writeOff := db.activateFile.WriteOff
	if err := db.activateFile.Write(encRecord); err != nil {
		return nil, err
	}
	// 用户自定义 是否写入数据时持久化
	if db.options.SyncWrites {
		if err := db.activateFile.Sync(); err != nil {
			return nil, err
		}
	}
	// 构建内存索引实例
	pos := &data.LogRecordPos{Fid: db.activateFile.FileId, Offset: writeOff}
	return pos, nil

}

// setActivateDataFile 设置新的活跃文件
func (db *DB) setActivateDataFile() error {
	var initialFiledId uint32 = 0
	if db.activateFile != nil {
		initialFiledId = db.activateFile.FileId + 1
	}
	// 打开新的文件
	dataFile, err := data.OpenDataFile(db.options.DirPath, initialFiledId)
	if err != nil {
		return err
	}
	db.activateFile = dataFile

	return nil
}
