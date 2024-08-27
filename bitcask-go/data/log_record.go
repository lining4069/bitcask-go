package data

//remark: 内存索引结构相关

type LogRecordType byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
)

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
