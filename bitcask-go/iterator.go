package bitcask_go

import (
	"bitcask-go/index"
	"bytes"
)

// remark 用户层面迭代器 （数据库DB结构体层面） 迭代器内元素是有序的

type Iterator struct {
	indexIter index.Iterator
	db        *DB
	options   IteratorOptions
}

func (it *Iterator) Rewind() {
	it.indexIter.Rewind()
	it.skipToNext()
}

func (it *Iterator) Seek(key []byte) {
	it.indexIter.Seek(key)
	it.skipToNext()
}

func (it *Iterator) Next() {
	it.indexIter.Next()
	it.skipToNext()
}

func (it *Iterator) Valid() bool {
	return it.indexIter.Valid()
}

func (it *Iterator) Key() []byte {
	return it.indexIter.Key()
}

func (it *Iterator) Value() ([]byte, error) {
	pos := it.indexIter.Value()
	it.db.mu.RLock()
	defer it.db.mu.RUnlock()
	value, err := it.db.getValueByPosition(pos)
	if err != nil {
		return nil, err
	}
	return value, err
}

func (it *Iterator) Close() {
	it.indexIter.Close()
}

func (it *Iterator) skipToNext() {
	// 不过滤
	prefixLen := len(it.options.Prefix)
	if prefixLen == 0 {
		return
	}
	// 过滤
	for ; it.indexIter.Valid(); it.indexIter.Next() {
		key := it.indexIter.Key()
		if prefixLen <= len(key) && bytes.Compare(it.options.Prefix, key[:prefixLen]) == 0 {
			break
		}
	}
}
