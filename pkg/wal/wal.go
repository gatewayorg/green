package wal

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	humanize "github.com/dustin/go-humanize"
	"github.com/gatewayorg/green/pkg/log"
	"github.com/golang/snappy"
)

const (
	sentinel          = 0
	DefaultFileBuffer = 2 << 22 // 4 MB
	maxEntrySize      = 2 << 24 // 16 MB, used to restrain the building of excessively large entry buffers in certain cases of file corruption
	CompressedSuffix  = ".snappy"
)

var (
	maxSegmentSize = int64(104857600)
	encoding       = binary.BigEndian
	sentinelBytes  = make([]byte, 4) // same as 0
)

type filebased struct {
	dir          string
	file         *os.File
	compressed   bool
	fileSequence int64
	position     int64
	fileFlags    int
	h            hash.Hash32
	log          log.Logger
}

func (fb *filebased) SetLogLevel(l log.Level) {
	fb.log.SetLevel(l)
}

func (fb *filebased) openFile() error {
	var err error
	if fb.file != nil {
		err = fb.file.Close()
		if err != nil {
			log.Errorf("Unable to close existing file %v: %v", fb.file.Name(), err)
		}
	}

	fb.compressed = false
	fb.file, err = os.OpenFile(fb.filename(), fb.fileFlags, 0600)

	if os.IsNotExist(err) {
		// Try compressed version
		fb.compressed = true
		fb.file, err = os.OpenFile(fb.filename()+CompressedSuffix, fb.fileFlags, 0600)
	}

	if err == nil {
		filename := fb.filename()
		seq := filenameToSequence(filename)
		ts := sequenceToTime(seq)
		log.Debugf("Opened(%v) %v (%v)", fb.fileFlags, filename, ts)
	}
	return err
}

func (fb *filebased) filename() string {
	return filepath.Join(fb.dir, sequenceToFilename(fb.fileSequence))
}

// WAL provides a simple write-ahead log backed by a single file on disk. It is
// safe to write to a single WAL from multiple goroutines.
type WAL struct {
	filebased
	syncImmediate bool
	writer        *bufio.Writer
	closed        bool
	bufChan       chan [][]byte
	mx            sync.RWMutex
	exit          chan struct{}
}

// Open opens a WAL in the given directory. It will be force synced to disk
// every syncInterval. If syncInterval is 0, it will force sync on every write
// to the WAL.
func Open(dir string, syncInterval time.Duration) (*WAL, error) {
	wal := &WAL{
		filebased: filebased{
			dir:       dir,
			fileFlags: os.O_CREATE | os.O_APPEND | os.O_WRONLY,
			h:         NewHash(),
			log:       log.New("wal"),
		},
		bufChan: make(chan [][]byte, 4096),
		exit:    make(chan struct{}),
	}
	err := wal.advance()
	if err != nil {
		return nil, err
	}

	if syncInterval <= 0 {
		wal.syncImmediate = true
	} else {
		go wal.sync(syncInterval)
	}

	go func() {
		for bufs := range wal.bufChan {
			wal.write(bufs...)
		}
		wal.exit <- struct{}{}
	}()

	return wal, nil
}

// Latest() returns the latest entry in the WAL along with its offset
func (wal *WAL) Latest() ([]byte, Offset, error) {
	var data []byte
	var offset Offset

	lastSeq := int64(0)
	err := wal.forEachSegmentInReverse(func(file os.FileInfo, first bool, last bool) (bool, error) {
		filename := file.Name()
		fileSequence := filenameToSequence(filename)
		if fileSequence == lastSeq {
			// Duplicate file (compressed vs uncompressed), ignore
			return true, nil
		}

		var r io.Reader
		r, err := os.OpenFile(filepath.Join(wal.dir, filename), os.O_RDONLY, 0600)
		if err != nil {
			return false, fmt.Errorf("Unable to open WAL file %v: %v", filename, err)
		}
		if strings.HasSuffix(filename, CompressedSuffix) {
			r = snappy.NewReader(r)
		} else {
			r = bufio.NewReaderSize(r, DefaultFileBuffer)
		}

		h := NewHash()
		lastPos := int64(0)
		position := int64(0)
		headBuf := make([]byte, 8)
		for {
			_, err := io.ReadFull(r, headBuf)
			if err != nil {
				break
			}

			length := int64(encoding.Uint32(headBuf))
			checksum := uint32(encoding.Uint32(headBuf[4:]))
			b := make([]byte, length)
			_, err = io.ReadFull(r, b)
			if err != nil {
				break
			}
			h.Reset()
			h.Write(b)
			if h.Sum32() != checksum {
				break
			}

			data = b
			lastPos = position
			position += 8 + length
		}

		if position > 0 {
			// We found a valid entry in the current file, return
			offset = newOffset(fileSequence, lastPos)
			return false, nil
		}

		lastSeq = fileSequence

		return true, nil
	})

	// No files found with a valid entry, return nil data and offset
	return data, offset, err
}

func (wal *WAL) Write(bufs ...[]byte) (int, error) {
	wal.bufChan <- bufs
	return 0, nil
}

// Write atomically writes one or more buffers to the WAL.
func (wal *WAL) write(bufs ...[]byte) (int, error) {
	wal.mx.Lock()
	defer wal.mx.Unlock()

	length := 0
	for _, b := range bufs {
		blen := len(b)
		if blen > maxEntrySize {
			fmt.Printf("Ignoring wal entry of size %v exceeding %v", humanize.Bytes(uint64(blen)), humanize.Bytes(uint64(maxEntrySize)))
			return 0, nil
		}
		length += blen
	}
	if length == 0 {
		return 0, nil
	}

	wal.h.Reset()
	for _, buf := range bufs {
		wal.h.Write(buf)
	}

	headerBuf := make([]byte, 4)

	// Write length
	encoding.PutUint32(headerBuf, uint32(length))
	n, err := wal.writer.Write(headerBuf)
	wal.position += int64(n)
	if err != nil {
		return 0, err
	}

	// Write checksum
	encoding.PutUint32(headerBuf, wal.h.Sum32())
	n, err = wal.writer.Write(headerBuf)
	wal.position += int64(n)
	if err != nil {
		return 0, err
	}

	for _, b := range bufs {
		n, err = wal.writer.Write(b)
		if err != nil {
			return 0, err
		}
		wal.position += int64(n)
	}

	if wal.syncImmediate {
		wal.doSync()
	}

	if wal.position >= maxSegmentSize {
		// Write sentinel length to mark end of file
		_, err = wal.writer.Write(sentinelBytes)
		if err != nil {
			return 0, err
		}
		err = wal.writer.Flush()
		if err != nil {
			return 0, err
		}
		err = wal.advance()
		if err != nil {
			return n, fmt.Errorf("Unable to advance to next file: %v", err)
		}
	}

	return n, nil
}

// TruncateBefore removes all data prior to the given offset from disk.
// The last segment will always be kept.
func (wal *WAL) TruncateBefore(o Offset) error {
	cutoff := sequenceToFilename(o.FileSequence())
	return wal.forEachSegment(func(file os.FileInfo, first bool, last bool) (bool, error) {
		if last || file.Name() >= cutoff {
			// Files are sorted by name, if we've gotten past the cutoff or
			// encountered the last (active) file, don't bother continuing.
			return false, nil
		}
		rmErr := os.Remove(filepath.Join(wal.dir, file.Name()))
		if rmErr != nil {
			return false, rmErr
		}
		wal.log.Debugf("Removed WAL file %v", filepath.Join(wal.dir, file.Name()))
		return true, nil
	})
}

// TruncateBeforeTime truncates WAL data prior to the given timestamp.
func (wal *WAL) TruncateBeforeTime(ts time.Time) error {
	return wal.TruncateBefore(newOffset(tsToFileSequence(ts), 0))
}

// TruncateToSize caps the size of the WAL to the given number of bytes
func (wal *WAL) TruncateToSize(limit int64) (int64, error) {
	remained := int64(0)
	err := wal.forEachSegmentInReverse(func(file os.FileInfo, first bool, last bool) (bool, error) {
		next := file.Size()
		if remained+next > limit {
			fullname := filepath.Join(wal.dir, file.Name())
			rmErr := os.Remove(fullname)
			if rmErr != nil {
				return false, rmErr
			}
			wal.log.Debugf("Removed WAL file %v", fullname)
		} else {
			remained += next
		}
		return true, nil
	})
	return remained, err
}

// CompressBefore compresses all data prior to the given offset on disk.
func (wal *WAL) CompressBefore(o Offset) error {
	cutoff := sequenceToFilename(o.FileSequence())
	return wal.forEachSegment(func(file os.FileInfo, first bool, last bool) (bool, error) {
		if last || file.Name() >= cutoff {
			// Files are sorted by name, if we've gotten past the cutoff or
			// encountered the last (active) file, don't bother continuing.
			return false, nil
		}
		return wal.compress(file)
	})
}

// CompressBeforeTime compresses all data prior to the given offset on disk.
func (wal *WAL) CompressBeforeTime(ts time.Time) error {
	return wal.CompressBefore(newOffset(tsToFileSequence(ts), 0))
}

// CompressBeforeSize compresses all segments prior to the given size
func (wal *WAL) CompressBeforeSize(limit int64) error {
	seen := int64(0)
	return wal.forEachSegmentInReverse(func(file os.FileInfo, first bool, last bool) (bool, error) {
		if last {
			// Don't compress the last (active) file
			return true, nil
		}
		next := file.Size()
		seen += next
		if seen > limit {
			return wal.compress(file)
		}
		return true, nil
	})
}

func (wal *WAL) compress(file os.FileInfo) (bool, error) {
	infile := filepath.Join(wal.dir, file.Name())
	outfile := infile + CompressedSuffix
	if strings.HasSuffix(file.Name(), CompressedSuffix) {
		// Already compressed
		return true, nil
	}
	in, err := os.OpenFile(infile, os.O_RDONLY, 0600)
	if err != nil {
		return false, fmt.Errorf("Unable to open input file %v for compression: %v", infile, err)
	}
	defer in.Close()
	out, err := ioutil.TempFile("", "")
	if err != nil {
		return false, fmt.Errorf("Unable to open temp file to compress %v: %v", infile, err)
	}
	defer out.Close()
	defer os.Remove(out.Name())
	compressedOut := snappy.NewWriter(out)
	_, err = io.Copy(compressedOut, bufio.NewReaderSize(in, DefaultFileBuffer))
	if err != nil {
		return false, fmt.Errorf("Unable to compress %v: %v", infile, err)
	}
	err = compressedOut.Close()
	if err != nil {
		return false, fmt.Errorf("Unable to finalize compression of %v: %v", infile, err)
	}
	err = out.Close()
	if err != nil {
		return false, fmt.Errorf("Unable to close compressed output %v: %v", outfile, err)
	}
	err = os.Rename(out.Name(), outfile)
	if err != nil {
		return false, fmt.Errorf("Unable to move compressed output %v to final destination %v: %v", out.Name(), outfile, err)
	}
	err = os.Remove(infile)
	if err != nil {
		return false, fmt.Errorf("Unable to remove uncompressed file %v: %v", infile, err)
	}
	wal.log.Debugf("Compressed WAL file %v", infile)
	return true, nil
}

func (wal *WAL) forEachSegment(cb func(file os.FileInfo, first bool, last bool) (bool, error)) error {
	files, err := ioutil.ReadDir(wal.dir)
	if err != nil {
		return fmt.Errorf("Unable to list log segments: %v", err)
	}

	for i, file := range files {
		more, err := cb(file, i == 0, i == len(files)-1)
		if !more || err != nil {
			return err
		}
	}

	return nil
}

func (wal *WAL) forEachSegmentInReverse(cb func(file os.FileInfo, first bool, last bool) (bool, error)) error {
	files, err := ioutil.ReadDir(wal.dir)
	if err != nil {
		return fmt.Errorf("Unable to list log segments: %v", err)
	}

	for i := len(files) - 1; i >= 0; i-- {
		more, err := cb(files[i], i == 0, i == len(files)-1)
		if !more || err != nil {
			return err
		}
	}

	return nil
}

// Close closes the wal, including flushing any unsaved writes.
func (wal *WAL) Close() error {
	wal.mx.Lock()
	close(wal.bufChan)
	<-wal.exit
	flushErr := wal.writer.Flush()
	syncErr := wal.file.Sync()
	wal.closed = true
	wal.mx.Unlock()
	closeErr := wal.file.Close()
	if flushErr != nil {
		return flushErr
	}
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}

func (wal *WAL) advance() error {
	wal.fileSequence = newFileSequence()
	wal.position = 0
	err := wal.openFile()
	if err == nil {
		wal.writer = bufio.NewWriterSize(wal.file, DefaultFileBuffer)
	}
	return err
}

func (wal *WAL) sync(syncInterval time.Duration) {
	for {
		time.Sleep(syncInterval)
		wal.mx.Lock()
		wal.doSync()
		wal.mx.Unlock()
	}
}

func (wal *WAL) doSync() {
	err := wal.writer.Flush()
	if err != nil {
		wal.log.Errorf("Unable to flush wal: %v", err)
		return
	}
	err = wal.file.Sync()
	if err != nil {
		wal.log.Errorf("Unable to sync wal: %v", err)
	}
}

func (wal *WAL) hasMovedBeyond(fileSequence int64) bool {
	wal.mx.RLock()
	hasMovedBeyond := wal.fileSequence > fileSequence
	wal.mx.RUnlock()
	return hasMovedBeyond
}

// Reader allows reading from a WAL. It is NOT safe to read from a single Reader
// from multiple goroutines.
type Reader struct {
	filebased
	wal          *WAL
	reader       io.Reader
	bufferSource func() []byte
	closed       int32
}

// NewReader constructs a new Reader for reading from this WAL starting at the
// given offset. The returned Reader is NOT safe for use from multiple
// goroutines. Name is just a label for the reader used during logging.
func (wal *WAL) NewReader(name string, offset Offset, bufferSource func() []byte) (*Reader, error) {
	r := &Reader{
		filebased: filebased{
			dir:       wal.dir,
			fileFlags: os.O_RDONLY,
			h:         NewHash(),
			log:       log.New(strings.Join([]string{"wal.", name}, "")),
		},
		wal:          wal,
		bufferSource: bufferSource,
	}
	if offset != nil {
		offsetString := sequenceToFilename(offset.FileSequence())
		if offsetString[0] != '0' {
			wal.log.Debugf("Converting legacy offset")
			offset = newOffset(offset.FileSequence()/1000, offset.Position())
		}

		files, err := ioutil.ReadDir(wal.dir)
		if err != nil {
			return nil, fmt.Errorf("Unable to list existing log files: %v", err)
		}

		cutoff := sequenceToFilename(offset.FileSequence())
		for _, fileInfo := range files {
			if fileInfo.Name() >= cutoff {
				// Found exist or more recent WAL file
				r.fileSequence = filenameToSequence(fileInfo.Name())
				if r.fileSequence == offset.FileSequence() {
					// Exact match, start at right position
					r.position = offset.Position()
				} else {
					// Newer WAL file, start at beginning
					r.position = 0
				}
				openErr := r.open()
				if openErr != nil {
					return nil, fmt.Errorf("Unable to open existing log file at %v: %v", fileInfo.Name(), openErr)
				}
				break
			}
		}
	}

	if r.file == nil {
		// Didn't find WAL file, advance
		err := r.advance()
		if err != nil {
			return nil, fmt.Errorf("Unable to advance initially: %v", err)
		}
		wal.log.Debugf("Replaying log starting at %v", r.file.Name())
	}
	return r, nil
}

// Read reads the next chunk from the WAL, blocking until one is available.
func (r *Reader) Read() ([]byte, error) {
	for {
		length, checksum, err := r.readHeader()
		if err != nil {
			return nil, err
		}
		if length > maxEntrySize {
			fmt.Printf("Discarding wal entry of size %v exceeding %v, probably corrupted\n", humanize.Bytes(uint64(length)), humanize.Bytes(uint64(maxEntrySize)))
			_, discardErr := io.CopyN(ioutil.Discard, r.reader, int64(length))
			if discardErr == io.EOF {
				discardErr = nil
			}
			return nil, discardErr
		}
		data, err := r.readData(int(length))
		if data != nil || err != nil {
			if data != nil {
				r.h.Reset()
				r.h.Write(data)
				if checksum != r.h.Sum32() {
					r.log.Errorf("Checksum mismatch, skipping entry")
					continue
				}
			}
			return data, err
		}
	}
}

func (r *Reader) readHeader() (length uint32, checksum uint32, err error) {
	headBuf := make([]byte, 8)
	for {
		for {
			if atomic.LoadInt32(&r.closed) == 1 {
				return 0, 0, io.ErrUnexpectedEOF
			}
			_, err = io.ReadFull(r.reader, headBuf)
			if err != nil && err.Error() == "EOF" {
				if r.wal.closed || r.wal.hasMovedBeyond(r.fileSequence) {
					break
				}

				if r.fileSequence == r.wal.fileSequence {
					return 0, 0, io.EOF
				}
			}
			if err != nil {
				r.log.Errorf("Unexpected error reading header from WAL file %v: %v", r.filename(), err)
				break
			}
			length = encoding.Uint32(headBuf[0:4])
			checksum = encoding.Uint32(headBuf[4:8])
			return
		}

		err := r.advance()
		if err != nil {
			return 0, 0, err
		}
	}
}

func (r *Reader) readData(length int) ([]byte, error) {
	buf := r.bufferSource()
	// Grow buffer if necessary
	if cap(buf) < length {
		buf = make([]byte, length)
	}

	// Set buffer length
	buf = buf[:length]

	// Read into buffer
	read := 0
	for {
		if atomic.LoadInt32(&r.closed) == 1 {
			return nil, io.ErrUnexpectedEOF
		}
		n, err := r.reader.Read(buf[read:])
		read += n
		r.position += int64(n)
		if err != nil && err.Error() == "EOF" && read < length {
			if r.wal.hasMovedBeyond(r.fileSequence) {
				r.log.Errorf("Out of data to read after reading %d, and WAL has moved beyond %d. Assuming WAL at %v corrupted. Advancing and continuing.", r.position, r.fileSequence, r.filename())
				advanceErr := r.advance()
				if advanceErr != nil {
					return nil, advanceErr
				}
				return nil, nil
			}
			// No newer log files, continue trying to read from this one
			time.Sleep(50 * time.Millisecond)
			continue
		}

		if err != nil {
			r.log.Errorf("Unexpected error reading data from WAL file %v: %v", r.filename(), err)
			return nil, nil
		}

		if read == length {
			return buf, nil
		}
	}
}

// Offset returns the furthest Offset read by this Reader. It is NOT safe to
// call this concurrently with Read().
func (r *Reader) Offset() Offset {
	return newOffset(r.fileSequence, r.position)
}

// Close closes the Reader.
func (r *Reader) Close() error {
	atomic.StoreInt32(&r.closed, 1)
	return r.file.Close()
}

func (r *Reader) open() error {
	err := r.openFile()
	if err != nil {
		return err
	}
	if r.compressed {
		r.reader = snappy.NewReader(r.file)
	} else {
		r.reader = bufio.NewReaderSize(r.file, DefaultFileBuffer)
	}
	if r.position > 0 {
		// Read to the correct offset
		// Note - we cannot just seek on the file because the data is compressed and
		// the recorded position does not correspond to a file offset.
		_, seekErr := io.CopyN(ioutil.Discard, r.reader, r.position)
		if seekErr != nil {
			return seekErr
		}
	}
	return nil
}

func (r *Reader) advance() error {
	r.log.Debugf("Advancing in %v", r.dir)
	for {
		if atomic.LoadInt32(&r.closed) == 1 {
			return io.ErrUnexpectedEOF
		}

		files, err := ioutil.ReadDir(r.dir)
		if err != nil {
			return fmt.Errorf("Unable to list existing log files: %v", err)
		}

		cutoff := sequenceToFilename(r.fileSequence)
		for _, fileInfo := range files {
			seq := filenameToSequence(fileInfo.Name())
			if seq == r.fileSequence {
				// Duplicate WAL segment (i.e. compressed vs uncompressed), ignore
				continue
			}
			if fileInfo.Name() > cutoff {
				// Files are sorted by name, if we've gotten past the cutoff, don't bother
				// continuing
				r.position = 0
				r.fileSequence = seq
				return r.open()
			}
		}

		time.Sleep(50 * time.Millisecond)
	}
}

func newFileSequence() int64 {
	return tsToFileSequence(time.Now())
}

func tsToFileSequence(ts time.Time) int64 {
	return ts.UnixNano() / 1000
}

func sequenceToFilename(seq int64) string {
	return fmt.Sprintf("%019d", seq)
}

func sequenceToTime(seq int64) time.Time {
	ts := seq * 1000
	s := ts / int64(time.Second)
	ns := ts % int64(time.Second)
	return time.Unix(s, ns)
}

func filenameToSequence(filename string) int64 {
	_, filePart := filepath.Split(filename)
	filePart = strings.TrimSuffix(filePart, CompressedSuffix)
	seq, err := strconv.ParseInt(filePart, 10, 64)
	if err != nil {
		fmt.Printf("Unparseable filename '%v': %v\n", filename, err)
		return 0
	}
	return seq
}

func NewHash() hash.Hash32 {
	return crc32.New(crc32.MakeTable(crc32.Castagnoli))
}
