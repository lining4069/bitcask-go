package data

import "encoding/binary"

//remark: 内存索引结构相关

type LogRecordType byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
)

// 数据文件中存储一条记录时，数据头部的长度
// crc，type，key_size,value_size
// 4+1+ 5*2 = 15
const MaxLogRecordHeaderSize = binary.MaxVarintLen32*2 + 4 + 1

type LogRecordHeader struct {
	crc        uint32        // 4 字节 数据校验值
	recordType LogRecordType // 1 字节 LogRecord 类型
	keySize    uint32        //LogRecord key长度
	valueSize  uint32        //LogRecord value 长度
}

// kv存储操作的数据单位
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}

// LogRecordPos 内存索引结构，指明数据在磁盘中的位置
type LogRecordPos struct {
	Fid    uint32 // 文件id
	Offset int64  // 数据在文件中的偏移量
}

// EncodeLogRecord 对LogRecord 实例进行编码,并
func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	return nil, 0
}

// decodeLogRecordHeader 解码 构建 头部实例 返回头部实例以及头部存储时实际所用长度
func decodeLogRecordHeader(encHeaderBuf []byte) (*LogRecordHeader, int64) {
	return nil, 0
}

// getLogRecordCRC 构建CRC家=校验值
func getLogRecordCRC(lr *LogRecord, header []byte) uint32 {
	return 0
}
