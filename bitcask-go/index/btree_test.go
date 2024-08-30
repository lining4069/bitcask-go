package index

import (
	"bitcask-go/data"
	"github.com/google/btree"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
)

func TestBTree_Put(t *testing.T) {
	bt := NewBTree()
	res := bt.Put(nil, &data.LogRecordPos{1, 100})
	assert.True(t, res)
}

func TestBTree_Iterator(t *testing.T) {
	type fields struct {
		tree   *btree.BTree
		lock   *sync.RWMutex
		values []*Item
	}
	type args struct {
		reverse bool
	}

	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			name: "reverse_true",

			fields: fields{
				tree: btree.New(32),
				lock: new(sync.RWMutex),
				values: []*Item{
					&Item{
						key: []byte("name1"), pos: &data.LogRecordPos{
							Fid:    0,
							Offset: 0,
						}},
					&Item{
						key: []byte("name2"),
						pos: &data.LogRecordPos{
							Fid:    0,
							Offset: 60,
						}},
				},
			},
			args: args{
				reverse: true,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bt := &BTree{
				tree: tt.fields.tree,
				lock: tt.fields.lock,
			}
			for _, item := range tt.fields.values {
				bt.Put(item.key, item.pos)
			}
			iterator := bt.Iterator(tt.args.reverse)
			assert.Equal(t, iterator.Key(), []byte("name2"))
			iterator.Next()
			assert.Equal(t, iterator.Key(), []byte("name1"))

			for iterator.Seek([]byte("name2")); iterator.Valid(); iterator.Next() {
				t.Log(string(iterator.Key()))
				t.Log(iterator.Value())
			}
		})
	}
}
