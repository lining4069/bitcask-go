package data

import (
	"encoding/binary"
	"hash/crc32"
)

//remark: 内存索引结构相关

type LogRecordType = byte

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

// EncodeLogRecord 对LogRecord 实例进行编码，用户传入的只有key-value键值对，需要在编码阶段增加header信息
// crc type   key_size      value_size    key   value
// 4    1   varint max(5)   varint max(5)  varint  varint
func EncodeLogRecord(lr *LogRecord) ([]byte, int64) {
	header := make([]byte, MaxLogRecordHeaderSize)
	// LogRecord 类型
	header[4] = lr.Type
	var index = 5

	index += binary.PutVarint(header[index:], int64(len(lr.Key)))
	index += binary.PutVarint(header[index:], int64(len(lr.Value)))

	var size = index + len(lr.Key) + len(lr.Value)

	encBytes := make([]byte, size)
	// copy header
	copy(encBytes[:index], header[:index])
	// copy key
	copy(encBytes[index:], lr.Key)
	// copy value
	copy(encBytes[index+len(lr.Key):], lr.Value)

	// 计算crc
	crc := crc32.ChecksumIEEE(encBytes[4:])
	// 小端存储 低地址存低值
	binary.LittleEndian.PutUint32(encBytes[:4], crc)

	return encBytes, int64(size)
}

// decodeLogRecordHeader 解码 构建 头部实例 返回头部实例以及头部存储时实际所用长度
func decodeLogRecordHeader(buf []byte) (*LogRecordHeader, int64) {
	if len(buf) <= 4 {
		return nil, 0
	}
	// 构建Header
	header := &LogRecordHeader{
		crc:        binary.LittleEndian.Uint32(buf[:4]),
		recordType: buf[4],
	}

	var index = 5
	// 取出实际的 key size
	keySize, n := binary.Varint(buf[index:])
	header.keySize = uint32(keySize)
	index += n

	// 取出实际的 value size
	valueSize, n := binary.Varint(buf[index:])
	header.valueSize = uint32(valueSize)
	index += n

	return header, int64(index)
}

// getLogRecordCRC 构建CRC家=校验值
func getLogRecordCRC(lr *LogRecord, header []byte) uint32 {
	if lr == nil {
		return 0
	}

	crc := crc32.ChecksumIEEE(header[:])
	crc = crc32.Update(crc, crc32.IEEETable, lr.Key)
	crc = crc32.Update(crc, crc32.IEEETable, lr.Value)

	return crc
}
