package data

import (
	"github.com/stretchr/testify/assert"
	"log"
	"os"
	"testing"
)

func TestOpenDataFile(t *testing.T) {
	dataFile, err := OpenDataFile(os.TempDir(), 0)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	dataFile1, err1 := OpenDataFile(os.TempDir(), 1111)
	assert.Nil(t, err1)
	assert.NotNil(t, dataFile1)

	log.Println(os.TempDir())

}

func TestDataFile_Write(t *testing.T) {
	dataFile, err1 := OpenDataFile(os.TempDir(), 0)
	assert.Nil(t, err1)
	assert.NotNil(t, dataFile)

	err2 := dataFile.Write([]byte("asas"))
	if err2 != nil {
		log.Println(err2)
	}
}
