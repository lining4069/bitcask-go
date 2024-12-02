package data

import (
	"bitcask-go/fio"
	"github.com/stretchr/testify/assert"
	"log"
	"os"
	"testing"
)

func TestOpenDataFile(t *testing.T) {
	dataFile, err := OpenDataFile(os.TempDir(), 0, fio.StandardFIO)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	dataFile1, err1 := OpenDataFile(os.TempDir(), 1111, fio.StandardFIO)
	assert.Nil(t, err1)
	assert.NotNil(t, dataFile1)

	log.Println(os.TempDir())

}

func TestDataFile_Write(t *testing.T) {
	dataFile, err1 := OpenDataFile(os.TempDir(), 0, fio.StandardFIO)
	assert.Nil(t, err1)
	assert.NotNil(t, dataFile)

	err2 := dataFile.Write([]byte("asas"))
	if err2 != nil {
		log.Println(err2)
	}
}

func TestDataFile_Sync(t *testing.T) {
	dataFile, err := OpenDataFile(os.TempDir(), 0, fio.StandardFIO)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	err = dataFile.Write([]byte("AASa"))
	assert.Nil(t, err)

	err = dataFile.Sync()
	assert.Nil(t, err)
}

func TestDataFile_ReadLogRecord(t *testing.T) {
	dataFile, err := OpenDataFile(os.TempDir(), 0, fio.StandardFIO)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	lr := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcask-go"),
		Type:  LogRecordNormal,
	}
	encLogRecord, length := EncodeLogRecord(lr)
	err = dataFile.Write(encLogRecord)
	assert.Nil(t, err)
	readLR, size, err := dataFile.ReadLogRecord(0)
	assert.NotNil(t, readLR)
	assert.Nil(t, err)
	assert.Equal(t, size, length)
	assert.Equal(t, lr, readLR)

	lr1 := &LogRecord{
		Key:   []byte("name-other"),
		Value: []byte("bitcask-go-other"),
		Type:  LogRecordNormal,
	}
	encLogRecord1, length1 := EncodeLogRecord(lr1)
	err1 := dataFile.Write(encLogRecord1)
	assert.Nil(t, err1)
	readLR1, size1, err1 := dataFile.ReadLogRecord(length)
	assert.NotNil(t, readLR1)
	assert.Nil(t, err1)
	assert.Equal(t, size1, length1)
	assert.Equal(t, lr1, readLR1)

	lr2 := &LogRecord{
		Key:   []byte("name-del"),
		Value: []byte("bitcask-go-del"),
		Type:  LogRecordDeleted,
	}
	encLogRecord2, length2 := EncodeLogRecord(lr2)

	err2 := dataFile.Write(encLogRecord2)
	assert.Nil(t, err2)

	readLR2, size2, err2 := dataFile.ReadLogRecord(length + length1)
	assert.NotNil(t, readLR2)
	assert.Nil(t, err2)

	assert.Equal(t, size2, length2)
	assert.Equal(t, lr2, readLR2)

}
