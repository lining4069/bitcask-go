package bitcask_go

import (
	"bitcask-go/data"
	"bitcask-go/index"
	"errors"
	"fmt"
	"github.com/gofrs/flock"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// Remark: KV存储模型

const (
	seqNoKey     = "seq.no" // 使用B+树索引引擎时，数据库关闭记录最新seqNo的数据记录的LogRecord中的Key值
	fileLockName = "flock"
)

// DB 核心KV存储模型 包含两个核心方法 PUT和GET方法
type DB struct {
	mu              *sync.RWMutex             // 读写锁
	activeFile      *data.DataFile            // 当前活跃数据文件 用于写入
	olderFiles      map[uint32]*data.DataFile // 旧文件
	options         Options                   // 配置
	index           index.Indexer             // 内存索引引擎
	fileIds         []int                     // 数据库加载环境数据内已存在的数据文件id数组，仅用于启动数据库
	seqNo           uint64                    // 事务序列号 全局递增
	isMerging       bool                      // 是否正在merge
	seqNoFileExists bool                      // 存储事务序列号的文件是否存在
	isInitial       bool                      // 是否是第一次初始化此数据目录
	fileLock        *flock.Flock              //数据库启动时对数据目录的文件锁
	bytesWrite      uint
}

// Open 打开数据库实例
func Open(options Options) (*DB, error) {
	// 传入自定义配置项校验
	if err := checkOptions(options); err != nil {
		return nil, err
	}
	// 传入的数据存储路径合法但不存在
	var isInitial bool
	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		isInitial = true
		if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}
	// 判断数据目录是否正在被使用，确保数据库实例在同一时刻只会被一个进程使用。
	fileLock := flock.New(filepath.Join(options.DirPath, fileLockName))
	hold, err := fileLock.TryLock()
	if err != nil {
		return nil, err
	}
	if !hold {
		return nil, ErrDatabaseIsUsing
	}
	//数据库实例目录存在但时为空，也是初始化
	entries, err := os.ReadDir(options.DirPath)
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		isInitial = true
	}
	// 创建数据库实例
	db := &DB{
		mu:         new(sync.RWMutex),
		options:    options,
		olderFiles: make(map[uint32]*data.DataFile),
		index:      index.NewIndexer(options.IndexType, options.DirPath, options.SyncWrites),
		isInitial:  isInitial,
		fileLock:   fileLock,
	}
	// 加载 merge 数据目录（即将merge操作产生的新的数据文件移动到当前数据实例的数据目录下）
	if err := db.loadMergeFiles(); err != nil {
		return nil, err
	}
	// 加载dirPath下的数据文件
	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}
	// B+树作为内存索引数据结构时不需要通过加载数据文件来加载索引
	if options.IndexType != BPlusTree {
		// 从 hint 索引文件中加载索引
		if err := db.loadIndexFromHintFile(); err != nil {
			return nil, err
		}

		// 遍历文件中所有记录，并更新到内存索引中
		if err := db.loadIndexFromDataFiles(); err != nil {
			return nil, err
		}
	}
	// 取出当前事务序列号
	if options.IndexType == BPlusTree {
		if err := db.loadSeqNo(); err != nil {
			return nil, err
		}
		if db.activeFile != nil {
			size, err := db.activeFile.IoManager.Size()
			if err != nil {
				return nil, err
			}
			db.activeFile.WriteOff = size
		}
	}
	return db, nil
}

// Put 写入方法 key不能为空
func (db *DB) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	// 构造KV模型数据存储实例
	logRecord := data.LogRecord{
		Key:   logRecordKeyWithSeq(key, nonTransactionSeqNo),
		Value: value,
		Type:  data.LogRecordNormal,
	}
	// 存储到磁盘中，并返回内存索引信息
	pos, err := db.appendLogRecordWithLock(&logRecord)
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
		return nil, ErrKeyIsEmpty
	}
	// 从内存索引信息中，获取数据的存储信息
	pos := db.index.Get(key)
	if pos == nil {
		return nil, ErrKeyNotFound
	}
	// 内存索引存在
	var dataFile *data.DataFile
	if db.activeFile.FileId == pos.Fid { // 要查询的数据文件为当前活跃文件
		dataFile = db.activeFile
	} else {
		dataFile = db.olderFiles[pos.Fid]
	}
	//存储文件不存在
	if dataFile == nil {
		return nil, ErrDataFileNotFound
	}
	// 根据偏移量读取数据构建logRecord
	logRecord, _, err := dataFile.ReadLogRecord(pos.Offset)
	if err != nil {
		return nil, err
	}
	// 处理当前查询数据为删除数据
	if logRecord.Type == data.LogRecordDeleted {
		return nil, ErrKeyNotFound
	}
	return logRecord.Value, err
}

// Delete 数据库删除数据
func (db *DB) Delete(key []byte) error {
	// key为空
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	// key不为空在内存索引中不存在
	if pos := db.index.Get(key); pos == nil {
		return nil
	}
	// 构建LogRecord 表明数据被删除
	logRecord := &data.LogRecord{
		Key:  logRecordKeyWithSeq(key, nonTransactionSeqNo),
		Type: data.LogRecordDeleted,
	}
	_, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return err
	}
	// 将被删除数据的内存索引删除
	if ok := db.index.Delete(key); !ok {
		return ErrIndexUpdateFailed
	}

	return nil
}

// Close 关闭数据库
func (db *DB) Close() error {
	defer func() {
		if err := db.fileLock.Unlock(); err != nil {
			panic(fmt.Sprintf("failed to unlock the directory, %v", err))
		}
	}()
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	// 关闭索引
	if err := db.index.Close(); err != nil {
		return err
	}
	// 保存当前事务序列号
	seqNoFile, err := data.OpenSeqNoFile(db.options.DirPath)
	if err != nil {
		return err
	}
	record := &data.LogRecord{
		Key:   []byte(seqNoKey),
		Value: []byte(strconv.FormatUint(db.seqNo, 10)),
	}
	encRecord, _ := data.EncodeLogRecord(record)
	if err := seqNoFile.Write(encRecord); err != nil {
		return err
	}
	if err := seqNoFile.Sync(); err != nil {
		return err
	}

	//关闭当前活跃文件
	if err := db.activeFile.Close(); err != nil {
		return err
	}
	// 关闭旧的额数据文件
	for _, file := range db.olderFiles {
		if err := file.Close(); err != nil {
			return err
		}
	}
	return nil
}

// Sync 持久化数据库 数据文件
func (db *DB) Sync() error {
	if db.activeFile == nil {
		return nil
	}

	db.mu.Lock()
	db.mu.Unlock()

	return db.activeFile.Sync()
}

// appendLogRecordWithLock 抽象出使其成为不需要加锁的操作，方便复用。
func (db *DB) appendLogRecordWithLock(dataRecord *data.LogRecord) (*data.LogRecordPos, error) {
	// 并发安全
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.appendLogRecord(dataRecord)
}

// appendLogRecord 存储操作主函数
func (db *DB) appendLogRecord(dataRecord *data.LogRecord) (*data.LogRecordPos, error) {
	// 存储 初始化 首次存储数据时，需要知道初始化，创建活跃文件
	if db.activeFile == nil {
		// 设置可用于写入和活跃文件
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}
	// 对写入数据编码获取长度
	encRecord, size := data.EncodeLogRecord(dataRecord)
	// 当前编码后数据长度大于剩余可存储容量
	// 处理：1、持久化当前活跃文件，2、打开新的活跃文件
	if db.activeFile.WriteOff+size > db.options.DataFileSize {
		// 持久化当前活跃文件
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
		// 将活跃文件转为旧文件
		db.olderFiles[db.activeFile.FileId] = db.activeFile
		// 打开新的活跃文件
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}
	// 获取到可用的获取文件
	// 写入数据
	writeOff := db.activeFile.WriteOff
	if err := db.activeFile.Write(encRecord); err != nil {
		return nil, err
	}

	// 数据库写入数据时，由用户自定义的SyncWrites以及是否进行持久化
	// 当写入数据的字节量大于options.BytesPerSync时将数据主动持久化
	db.bytesWrite += uint(size)
	// 用户自定义 是否写入数据时持久化
	var needSync = db.options.SyncWrites
	if !needSync && db.options.BytesPerSync > 0 && db.bytesWrite >= db.options.BytesPerSync {
		needSync = true
	}
	// 数据持久化
	if needSync {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
		// 重置当前写入数据计数器
		if db.bytesWrite > 0 {
			db.bytesWrite = 0
		}
	}
	// 构建内存索引实例
	pos := &data.LogRecordPos{Fid: db.activeFile.FileId, Offset: writeOff}
	return pos, nil

}

// setActiveDataFile 设置新的活跃文件
func (db *DB) setActiveDataFile() error {
	var initialFiledId uint32 = 0
	if db.activeFile != nil {
		initialFiledId = db.activeFile.FileId + 1
	}
	// 打开新的文件
	dataFile, err := data.OpenDataFile(db.options.DirPath, initialFiledId)
	if err != nil {
		return err
	}
	db.activeFile = dataFile

	return nil
}

// loadDataFiles 根据启动路径加载路径下所有数据文件
func (db *DB) loadDataFiles() error {
	dirEntries, err := os.ReadDir(db.options.DirPath)
	if err != nil {
		return err
	}
	var fileIds []int
	// 读取文件夹下数据，构建文件id数组
	for _, entry := range dirEntries {
		if strings.HasSuffix(entry.Name(), data.DataFileNameSuffix) {
			splitNames := strings.Split(entry.Name(), ".")
			fileId, err := strconv.Atoi(splitNames[0])
			if err != nil {
				return ErrDataDirectoryCorrupted
			}
			fileIds = append(fileIds, fileId)
		}
	}

	// 对文件id进行排序，从小到大进行加载
	sort.Ints(fileIds)
	// 将排序后的启动时文件id赋值
	db.fileIds = fileIds
	// 遍历灭一个id，打开对应的数据文件
	for i, fid := range fileIds {
		dataFile, err := data.OpenDataFile(db.options.DirPath, uint32(fid))
		if err != nil {
			return err
		}
		if i == len(fileIds)-1 {
			db.activeFile = dataFile
		} else {
			db.olderFiles[uint32(fid)] = dataFile
		}
	}
	return nil
}

// loadIndexFromDataFiles 根据数据文件，构建内存中索引
func (db *DB) loadIndexFromDataFiles() error {
	if len(db.fileIds) == 0 { // 当前数据库为空数据库
		return nil
	}

	// 查看是否发生过 merge
	hasMerge, nonMergeFileId := false, uint32(0) // 是否merge过，没有被merge的最小的Fid
	mergeFinFileName := filepath.Join(db.options.DirPath, data.MergeFinishedFileName)
	if _, err := os.Stat(mergeFinFileName); err == nil {
		fid, err := db.getNonMergeFileId(db.options.DirPath)
		if err != nil {
			return err
		}
		hasMerge = true
		nonMergeFileId = fid
	}

	updateIndex := func(key []byte, typ data.LogRecordType, pos *data.LogRecordPos) {
		var ok bool
		if typ == data.LogRecordDeleted {
			ok = db.index.Delete(key)
		} else {
			ok = db.index.Put(key, pos)
		}
		if !ok {
			panic("failed to update index at startup")
		}
	}

	// 暂存事务数据
	transactionRecords := make(map[uint64][]*data.TransactionRecord)
	var currentSeqNo = nonTransactionSeqNo

	for i, fid := range db.fileIds {
		var fileId = uint32(fid)
		// 如果比最近未参与 merge 的文件 id 更小，则说明已经从 Hint 文件中加载索引了
		if hasMerge && fileId < nonMergeFileId {
			continue
		}
		var dataFile *data.DataFile
		if db.activeFile.FileId == fileId {
			dataFile = db.activeFile
		} else {
			dataFile = db.olderFiles[fileId]
		}

		// 循序处理当前文件内的内容，构建LogRecord，进而更新数据库时启动内存索引信息
		var offset int64 = 0
		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}

			// 构造内存索引并保存
			logRecordPos := &data.LogRecordPos{Fid: fileId, Offset: offset}

			// 解析 key，拿到事务序列号
			realKey, seqNo := parseLogRecordKey(logRecord.Key)
			if seqNo == nonTransactionSeqNo {
				// 非事务操作，直接更新内存索引
				updateIndex(realKey, logRecord.Type, logRecordPos)
			} else {
				// 事务完成，对应的 seq no 的数据可以更新到内存索引中
				if logRecord.Type == data.LogRecordTxnFinished {
					for _, txnRecord := range transactionRecords[seqNo] {
						updateIndex(txnRecord.Record.Key, txnRecord.Record.Type, txnRecord.Pos)
					}
					delete(transactionRecords, seqNo)
				} else {
					logRecord.Key = realKey
					transactionRecords[seqNo] = append(transactionRecords[seqNo], &data.TransactionRecord{
						Record: logRecord,
						Pos:    logRecordPos,
					})
				}
			}

			// 更新事务序列号
			if seqNo > currentSeqNo {
				currentSeqNo = seqNo
			}

			// 递增 offset，下一次从新的位置开始读取
			offset += size
		}
		// 处理到最后一个处理的文件 即为当前活跃文件,更新其WriteOff
		if i == len(db.fileIds)-1 {
			db.activeFile.WriteOff = offset
		}
	}

	// 更新事务序列号
	db.seqNo = currentSeqNo
	return nil
}

//KV数据库 用户层面迭代器

// getValueByPosition 根据内存索引信息获取 value
func (db *DB) getValueByPosition(logRecordPos *data.LogRecordPos) ([]byte, error) {
	var dataFile *data.DataFile
	if db.activeFile.FileId == logRecordPos.Fid {
		dataFile = db.activeFile
	} else {
		dataFile = db.olderFiles[logRecordPos.Fid]
	}
	if dataFile == nil {
		return nil, ErrDataFileNotFound
	}
	logRecord, _, err := dataFile.ReadLogRecord(logRecordPos.Offset)
	if err != nil {
		return nil, err
	}
	if logRecord.Type == data.LogRecordDeleted {
		return nil, ErrKeyNotFound
	}
	return logRecord.Value, nil
}

// ListKeys 获取数据库中所有的key
func (db *DB) ListKeys() [][]byte {
	iterator := db.index.Iterator(false)
	keys := make([][]byte, db.index.Size())
	var idx int
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		keys[idx] = iterator.Key()
		idx++
	}
	return keys
}

// Fold 获取所有的数据(key-value 键值对)，并执行用户指定的操作，返回false时终止遍历
func (db *DB) Fold(fn func(key []byte, value []byte) bool) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	iterator := db.index.Iterator(false)
	// B+树索引引擎 blotdb 是允许一个进程访问需要关闭防止阻塞
	defer iterator.Close()
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		value, err := db.getValueByPosition(iterator.Value())
		if err != nil {
			return err
		}
		if !fn(iterator.Key(), value) {
			break
		}
	}

	return nil
}

// 数据库操作utils 方法

// checkOptions 校验配置项是否合法
func checkOptions(options Options) error {
	if options.DirPath == "" {
		return errors.New("database dirPath is empty")
	}
	if options.DataFileSize <= 0 {
		return errors.New(" database dataFileSize must greater than 0")
	}
	return nil
}

// B+树索引引擎下加载数据库上一次Close生成的seqNo持久化文件
func (db *DB) loadSeqNo() error {
	fileName := filepath.Join(db.options.DirPath, data.SeqNoFileName)
	if _, err := os.Stat(fileName); os.IsNotExist(err) {
		return nil
	}

	seqNoFile, err := data.OpenSeqNoFile(db.options.DirPath)
	if err != nil {
		return err
	}
	record, _, err := seqNoFile.ReadLogRecord(0)
	seqNo, err := strconv.ParseUint(string(record.Value), 10, 64)
	if err != nil {
		return err
	}
	db.seqNo = seqNo
	db.seqNoFileExists = true

	return os.Remove(fileName)
}
