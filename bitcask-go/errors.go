package bitcask_go

// Remark:自定义异常
import "errors"

var (
	ErrorKeyIsEmpty           = errors.New("key is not null")
	ErrIndexUpdateFailed      = errors.New("update index failed")
	ErrKeyNotFound            = errors.New("the key in not in database")
	ErrDataFileNotFound       = errors.New("file not found")
	ErrDataDirectoryCorrupted = errors.New("the directory may be corrupted")
	ErrExceedMaxBatchNum      = errors.New("exceed the max batch num")
	ErrMergeIsProgress        = errors.New("merge is in progress, try again later")
	ErrDatabaseIsUsing        = errors.New("the database directory is used by another process")
)
