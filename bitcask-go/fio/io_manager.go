package fio

// remark: 磁盘io接口

const DataFilePerm = 0644

type FileIOType = byte

const (
	StandardFIO FileIOType = iota
	MemoryMap
)

// IOManager 抽象出接口，可以扩展不同的磁盘存储操作的实现
type IOManager interface {
	// Read  从文件给定位置获取对应的数值
	Read([]byte, int64) (int, error)
	// Write 写入字节数据到文件
	Write([]byte) (int, error)
	//Sync 持久化数据
	Sync() error
	// Close 关闭文件
	Close() error
	// Size 获取文件长度
	Size() (int64, error)
}

// NewIOManager 初始化 IOManager，目前只支持标准 FileIO,以及启动加速用到的mmap
func NewIOManager(fileName string, ioType FileIOType) (IOManager, error) {
	switch ioType {
	case StandardFIO:
		return NewFileIOManger(fileName)
	case MemoryMap:
		return NewMMapIOManager(fileName)
	default:
		panic("unsupported io type")
	}
}
