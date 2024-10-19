package bitcask_go

import (
	"bitcask-go/utils"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDb_WriteBatch(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "./tmp/bitCask-go-iterator"
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	wb := db.NewWriteBatch(DefaultWriteBatchOptions)
	err = wb.Put(utils.GetTestKey(1), utils.RandomValue(10))
	assert.Nil(t, err)
	err = wb.Delete(utils.GetTestKey(2))
	assert.Nil(t, err)

	// without Commit
	_, err = db.Get(utils.GetTestKey(1))
	assert.Equal(t, ErrKeyNotFound, err)

	// 提交
	err = wb.Commit()
	assert.Nil(t, err)

	val1, err := db.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	t.Log(val1)

	// 删除数据
	wb1 := db.NewWriteBatch(DefaultWriteBatchOptions)
	err = wb1.Delete(utils.GetTestKey(1))
	err = wb1.Commit()
	assert.Nil(t, err)

	val2, err2 := db.Get(utils.GetTestKey(1))
	assert.Equal(t, ErrKeyNotFound, err2)
	t.Log(val2)

}
func TestDB_NewWriteBatch_2(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "./tmp/bitCask_go_batch_2"
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(utils.GetTestKey(1), utils.RandomValue(10))
	assert.Nil(t, err)

	wb := db.NewWriteBatch(DefaultWriteBatchOptions)
	err = wb.Put(utils.GetTestKey(2), utils.RandomValue(10))
	assert.Nil(t, err)

	err = wb.Delete(utils.GetTestKey(1))
	assert.Nil(t, err)

	err = wb.Commit()
	assert.Nil(t, err)

	err = wb.Put(utils.GetTestKey(11), utils.RandomValue(10))
	assert.Nil(t, err)
	err = wb.Commit()
	assert.Nil(t, err)

	// 重启
	err = db.Close()
	assert.Nil(t, err)

	db2, err := Open(opts)
	assert.Nil(t, err)

	_, err = db2.Get(utils.GetTestKey(1333))
	assert.Equal(t, ErrKeyNotFound, err)

	// 校验序列号
	assert.Equal(t, uint64(2), db2.seqNo)

	iterOpts := DefaultIteratorOptions
	iter3 := db2.NewIterator(iterOpts)
	for iter3.Rewind(); iter3.Valid(); iter3.Next() {
		t.Log("value", string(iter3.Key()))
	}
}
