package index

import (
	"bitcask-go/data"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBTree_Put(t *testing.T) {
	bt := NewBTree()
	res := bt.Put(nil, &data.LogRecordPos{1, 100})
	assert.True(t, res)
}
