package data

import (
	"bitcask-go/fio"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"path/filepath"
)

// Remark : 数据文件抽象 数据文件和IOManager 上下层调用关系

var (
	ErrInvalidCRC = errors.New("invalid crc,log record wrong")
)

// 数据文件默认后缀
const DataFileNameSuffix string = ".data"

// DataFile 数据文件
type DataFile struct {
	FileId    uint32
	WriteOff  int64
	IoManager fio.IOManager
}

// Sync 数据文件持久化
func (df *DataFile) Sync() error {
	return df.IoManager.Sync()
}

// 关闭
func (df *DataFile) Close() error {
	return df.IoManager.Close()
}

// Write 将编码后的logRecord的byte数据写入数据文件
func (df *DataFile) Write(encRecord []byte) error {
	nBytes, err := df.IoManager.Write(encRecord)
	if err != nil {
		return err
	}
	df.WriteOff += int64(nBytes)

	return nil
}

// ReadLogRecord 根据offset 读取数据文件，构建LogRecord
func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	fileSize, errSize := df.IoManager.Size()
	if errSize != nil {
		return nil, 0, errSize
	}
	var headerBytes int64 = MaxLogRecordHeaderSize
	if offset+MaxLogRecordHeaderSize > fileSize {
		headerBytes = fileSize - offset
	}
	// 读取定长数据头部信息
	encHeaderBuf, errReadBytes := df.readNBytes(headerBytes, offset)
	if errReadBytes != nil {
		return nil, 0, errReadBytes
	}
	// 对数据头部信息解码
	header, headerSize := decodeLogRecordHeader(encHeaderBuf)

	// 下面两种判断表示读取到了文件末尾 看后面为什么会存在读取数据构建的header crc，keySize,valueSize为0
	if header == nil {
		return nil, 0, io.EOF
	}
	if header.crc == 0 && header.keySize == 0 && header.valueSize == 0 {
		return nil, 0, io.EOF
	}

	keySize, valueSize := int64(header.keySize), int64(header.valueSize)
	var recordSize = headerSize + keySize + valueSize

	logRecord := &LogRecord{Type: header.recordType}

	if keySize > 0 || valueSize > 0 {
		kvBuf, err := df.readNBytes(keySize+valueSize, offset+headerSize)
		if err != nil {
			return nil, 0, err
		}

		logRecord.Key = kvBuf[:keySize]
		logRecord.Value = kvBuf[keySize:]
	}
	// 根据解析后得到的LogRecord以及header数据计算crc，与header.crc比较
	crc := getLogRecordCRC(logRecord, encHeaderBuf[crc32.Size:headerSize])
	if crc != header.crc {
		return nil, 0, ErrInvalidCRC
	}

	return logRecord, recordSize, nil
}

// readNBytes 对datafile从指定偏移量开始读取指定长度的数据
func (df *DataFile) readNBytes(nSize int64, offset int64) (b []byte, err error) {
	b = make([]byte, nSize)
	_, err = df.IoManager.Read(b, offset)
	return
}

// OpenDataFile 打开新的数据文件
func OpenDataFile(dirPath string, fileId uint32) (*DataFile, error) {
	fileName := filepath.Join(dirPath, fmt.Sprintf("%09d", fileId)+DataFileNameSuffix)
	// 初始化io管理接口
	ioManager, err := fio.NewFileIOManger(fileName)
	if err != nil {
		return nil, err
	}
	return &DataFile{
		FileId:    fileId,
		WriteOff:  0, // 所以在loadIndexFromDataFiles 中需要处理活跃文件（最新文件）的WriteOff
		IoManager: ioManager,
	}, nil
}
