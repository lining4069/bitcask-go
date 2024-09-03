package bitcask_go

import (
	"bitcask-go/utils"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDB_NewIterator(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "./tmp/bitCask-go-iterator"
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	iterator := db.NewIterator(DefaultIteratorOptions)
	assert.NotNil(t, iterator)
	assert.Equal(t, false, iterator.Valid())

}

func TestDB_Iterator_One_Value(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "./tmp/bitCask-go-iterator"
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(utils.GetTestKey(10), utils.GetTestKey(10))
	assert.Nil(t, err)

	iterator := db.NewIterator(DefaultIteratorOptions)
	assert.NotNil(t, iterator)
	assert.Equal(t, true, iterator.Valid())

	value, err := iterator.Value()
	assert.Nil(t, err)
	t.Log(string(iterator.Key()))
	t.Log(string(value))
	assert.Equal(t, utils.GetTestKey(10), iterator.Key())
	assert.Equal(t, utils.GetTestKey(10), value)

}

func TestDB_Iterator_Multi_Values(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "./tmp/bitCask-go-iterator"
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put([]byte("annde"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("bnnde"), utils.RandomValue(10))
	assert.Nil(t, err)

	err = db.Put([]byte("dnnde"), utils.RandomValue(10))
	assert.Nil(t, err)

	err = db.Put([]byte("cnnde"), utils.RandomValue(10))
	assert.Nil(t, err)

	iter1 := db.NewIterator(DefaultIteratorOptions)
	for iter1.Rewind(); iter1.Valid(); iter1.Next() {
		t.Log("key=", string(iter1.Key()))
	}
	// 迭代器下表回到起始位置
	iter1.Rewind()

	for iter1.Seek([]byte("b")); iter1.Valid(); iter1.Next() {
		t.Log("suffix b key=", string(iter1.Key()))
	}
	// 倒序
	iterOptions := DefaultIteratorOptions
	iterOptions.Reverse = true
	iter2 := db.NewIterator(iterOptions)
	for iter2.Rewind(); iter2.Valid(); iter2.Next() {
		t.Log("Reverse key=", string(iter2.Key()))
	}
	// 倒序 seek
	iter2.Rewind()
	for iter2.Seek([]byte("b")); iter2.Valid(); iter2.Next() {
		t.Log("reverse suffix b key=", string(iter2.Key()))
	}
	// 充值index
	iter2.Rewind()

}

func TestDB_Iterator_Multi_Values_Prefix(t *testing.T) { // 测试数据库只操作包含指定prefix的key
	// 启动数据库
	opts := DefaultOptions
	opts.DirPath = "./tmp/bitCask-go-iterator"
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)
	// 增加测试数据
	err = db.Put([]byte("annde"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("bnnde"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("dnnde"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("cnnde"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("bnnde-other"), utils.RandomValue(10))
	assert.Nil(t, err)

	// 带prefix配置的构建索引迭代器
	iterOpts := DefaultIteratorOptions
	iterOpts.Prefix = []byte("b")
	iter3 := db.NewIterator(iterOpts)
	for iter3.Rewind(); iter3.Valid(); iter3.Next() {
		t.Log("option with prefix key=", string(iter3.Key()))
	}
}
