package data

import "testing"

func TestEncodeLogRecord(t *testing.T) {
	lr := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcask-go"),
		Type:  LogRecordNormal,
	}
	encLogRecord, length := EncodeLogRecord(lr)

	t.Log(encLogRecord)
	t.Log(length)
}
