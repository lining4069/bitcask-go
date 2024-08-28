package bitcask_go

// Remark:自定义异常
import "errors"

var (
	ErrorKeyIsEmpty           = errors.New("key is not null")
	ErrIndexUpdateFailed      = errors.New("update index failed")
	ErrKeyNotFound            = errors.New("the key in not in database")
	ErrDataFileNotFound       = errors.New("File is not found")
	ErrDataDirectoryCorrupted = errors.New("the directory may be corrupted")
)
