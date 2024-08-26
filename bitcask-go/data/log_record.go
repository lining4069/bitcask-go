package data

//remark: 内存索引结构相关

// LogRecordPos 内存索引结构，指明数据在磁盘中的位置
type LogRecordPos struct {
	Fid    uint32 // 文件id
	Offset int64  // 数据在文件中的偏移量
}
