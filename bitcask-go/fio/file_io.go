package fio

// remark: 使用基础io实现磁盘操作接口
import "os"

type FileIO struct {
	fd *os.File
}

func NewFileIOManger(fileName string) (*FileIO, error) {
	fd, err := os.OpenFile(
		fileName,
		os.O_CREATE|os.O_RDWR|os.O_RDONLY,
		DataFilePerm,
	)
	if err != nil {
		return nil, err
	}

	return &FileIO{fd: fd}, nil
}

// 实现IOManager接口

func (fio *FileIO) Read(bytes []byte, offset int64) (int, error) {
	return fio.fd.ReadAt(bytes, offset)
}

func (fio *FileIO) Write(bytes []byte) (int, error) {
	return fio.fd.Write(bytes)
}

func (fio *FileIO) Sync() error {
	return fio.fd.Sync()
}

func (fio *FileIO) Close() error {
	return fio.fd.Close()
}
