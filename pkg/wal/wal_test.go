package wal

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gatewayorg/green/pkg/log"
	"github.com/golang/snappy"
	"github.com/oxtoacart/bpool"
	"github.com/stretchr/testify/assert"
)

func TestFileNaming(t *testing.T) {
	seq := newFileSequence()
	filename := filepath.Join("folder", sequenceToFilename(seq))
	assert.Equal(t, seq, filenameToSequence(filename))
	filename = filename + CompressedSuffix
	assert.Equal(t, seq, filenameToSequence(filename))
}

func TestOffsetAfter(t *testing.T) {
	assert.True(t, newOffset(0, 1).After(nil))
	assert.False(t, Offset(nil).After(newOffset(0, 1)))

	assert.True(t, newOffset(1, 0).After(nil))
	assert.False(t, Offset(nil).After(newOffset(1, 0)))

	assert.True(t, newOffset(1, 50).After(newOffset(1, 0)))
	assert.False(t, newOffset(1, 0).After(newOffset(1, 50)))

	assert.True(t, newOffset(2, 0).After(newOffset(1, 50)))
	assert.False(t, newOffset(1, 50).After(newOffset(2, 0)))

	assert.False(t, Offset(nil).After(Offset(nil)))
	assert.False(t, newOffset(1, 50).After(newOffset(1, 50)))
}

func TestWAL(t *testing.T) {
	origMaxSegmentSize := maxSegmentSize
	defer func() {
		maxSegmentSize = origMaxSegmentSize
	}()
	maxSegmentSize = 5

	dir, err := ioutil.TempDir("", "waltest")
	if !assert.NoError(t, err) {
		return
	}
	defer os.RemoveAll(dir)

	wal, err := Open(dir, 0)
	if !assert.NoError(t, err) {
		return
	}
	defer wal.Close()

	wal.SetLogLevel(log.LevelDebug)

	bufferPool := bpool.NewBytePool(1, 65536)
	r, err := wal.NewReader("test", nil, bufferPool.Get)
	if !assert.NoError(t, err) {
		return
	}
	defer r.Close()

	testReadWrite := func(val string) bool {
		wal.log.Debug(1)
		n, readErr := wal.Write([]byte(val))
		if !assert.NoError(t, readErr) {
			return false
		}
		if !assert.Equal(t, 1, n) {
			return false
		}

		wal.log.Debug(2)
		wal.log.Debug(fmt.Sprintf("r.Offset() %v", r.Offset()))
		b, readErr := r.Read()
		wal.log.Debug(fmt.Sprintf("r.Offset() %v", r.Offset()))
		if !assert.NoError(t, readErr) {
			return false
		}
		if !assert.Equal(t, len(val), n) {
			return false
		}
		if !assert.Equal(t, val, string(b[:1])) {
			return false
		}
		wal.log.Debug(3)

		return true
	}

	if !testReadWrite("1") {
		return
	}
	if !testReadWrite("2") {
		return
	}

	// Reopen WAL
	wal.Close()
	wal, err = Open(dir, 0)
	if !assert.NoError(t, err) {
		return
	}
	defer wal.Close()
	latest, lc, err := wal.Latest()
	if !assert.NoError(t, err) {
		return
	}
	assert.EqualValues(t, 0, lc.Position())
	assert.Equal(t, "2", string(latest))

	r2, err := wal.NewReader("test", r.Offset(), bufferPool.Get)
	if !assert.NoError(t, err) {
		return
	}
	defer r2.Close()
	wal.log.Debug("Problem is here")
	wal.log.Debug(fmt.Sprintf("r.Offset() %v", r.Offset()))
	wal.log.Debug(fmt.Sprintf("r2.Offset() %v", r2.Offset()))
	// Problem is here
	if !testReadWrite("3") {
		return
	}

	// Compress item 1
	err = wal.CompressBefore(r2.Offset())
	if !assert.NoError(t, err) {
		return
	}

	assertWALContents := func(entries []string) {
		// Read the full WAL again
		r, err = wal.NewReader("test", nil, bufferPool.Get)
		if !assert.NoError(t, err) {
			return
		}
		defer r.Close()

		for _, expected := range entries {
			b, readErr := r.Read()
			if !assert.NoError(t, readErr) {
				return
			}
			if !assert.Equal(t, expected, string(b)) {
				return
			}
		}
	}

	assertWALContents([]string{"1", "2", "3"})

	// Corrupt the Snappy WAL file
	files, _ := ioutil.ReadDir(dir)
	for _, fi := range files {
		name := filepath.Join(dir, fi.Name())
		file, _ := os.OpenFile(name, os.O_RDWR, 0644)
		if strings.HasSuffix(name, CompressedSuffix) {
			w := snappy.NewWriter(file)
			lenBuf := make([]byte, 4)
			encoding.PutUint32(lenBuf, 100)
			_, err := w.Write(lenBuf)
			if err != nil {
				panic(err)
			}
			w.Flush()
			file.Write([]byte("garbage"))
			wal.log.Debug(fmt.Sprintf("corrupted file %v", name))
		}
		file.Close()
	}

	assertWALContents([]string{"2", "3"})

	// Reader opened at prior offset should only get "3"
	b, readErr := r2.Read()
	if !assert.NoError(t, readErr) {
		return
	}
	if !assert.Equal(t, "3", string(b)) {
		return
	}

	_, err = wal.Write([]byte("data to force new WAL"))
	if !assert.NoError(t, err) {
		return
	}

	// Truncate as of known offset, should not delete any files
	truncateErr := wal.TruncateBefore(r.Offset())
	testTruncate(t, wal, truncateErr, 3)

	// Truncate as of now, which should remove old log segment
	truncateErr = wal.TruncateBeforeTime(time.Now())
	testTruncate(t, wal, truncateErr, 1)

	// Truncate to size 1, which should remove remaining log segment
	var remained int64
	remained, truncateErr = wal.TruncateToSize(1)
	assert.NoError(t, truncateErr, "Should be able to truncate")
	assert.Equal(t, int64(0), remained)
}

func testTruncate(t *testing.T, wal *WAL, err error, expectedSegments int) {
	if assert.NoError(t, err, "Should be able to truncate") {
		segments, err := ioutil.ReadDir(wal.dir)
		if assert.NoError(t, err, "Should be able to list segments") {
			assert.Equal(t, expectedSegments, len(segments))
		}
	}
}
