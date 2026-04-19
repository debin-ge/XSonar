package internal

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
)

type ndjsonWriter struct {
	partPath     string
	finalPath    string
	file         *os.File
	buffer       *bufio.Writer
	flushEvery   int
	pendingLines int
	fsyncOnClose bool
	closed       bool
	committed    bool
}

func newNDJSONWriter(finalPath string, flushEvery int, fsyncOnClose bool) (*ndjsonWriter, error) {
	finalPath = filepath.Clean(finalPath)
	partPath := finalPath + ".part"
	if err := os.MkdirAll(filepath.Dir(finalPath), 0o750); err != nil {
		return nil, err
	}

	// #nosec G304 -- partPath is derived from the worker-controlled output path.
	file, err := os.OpenFile(partPath, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err := lockNDJSONFile(file, syscall.LOCK_EX); err != nil {
		_ = file.Close()
		return nil, err
	}

	return &ndjsonWriter{
		partPath:     partPath,
		finalPath:    finalPath,
		file:         file,
		buffer:       bufio.NewWriter(file),
		flushEvery:   normalizeNDJSONFlushEvery(flushEvery),
		fsyncOnClose: fsyncOnClose,
	}, nil
}

func (w *ndjsonWriter) Append(record any) error {
	if w == nil || w.file == nil {
		return errors.New("ndjson writer is not open")
	}
	if w.closed || w.committed {
		return errors.New("ndjson writer is closed")
	}

	line, err := json.Marshal(record)
	if err != nil {
		return err
	}
	line = append(line, '\n')
	if _, err = w.buffer.Write(line); err != nil {
		return err
	}

	w.pendingLines++
	if w.pendingLines < w.flushEvery {
		return nil
	}
	return w.flushBuffer()
}

func (w *ndjsonWriter) AppendAndFlush(record any) error {
	if err := w.Append(record); err != nil {
		return err
	}
	return w.Flush()
}

func (w *ndjsonWriter) Flush() error {
	if w == nil {
		return nil
	}
	return w.flushBuffer()
}

func (w *ndjsonWriter) Commit() error {
	if w == nil {
		return nil
	}
	if w.committed {
		return nil
	}

	if err := w.closeFile(); err != nil {
		return err
	}
	if err := os.Rename(w.partPath, w.finalPath); err != nil {
		return err
	}
	w.committed = true
	return nil
}

func (w *ndjsonWriter) Abort() error {
	if w == nil {
		return nil
	}

	if err := w.closeFile(); err != nil {
		return err
	}
	if err := os.Remove(w.partPath); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func (w *ndjsonWriter) Close() error {
	if w == nil {
		return nil
	}
	return w.closeFile()
}

func (w *ndjsonWriter) closeFile() error {
	if w == nil || w.file == nil || w.closed {
		return nil
	}

	if err := w.flushBuffer(); err != nil {
		_ = lockNDJSONFile(w.file, syscall.LOCK_UN)
		_ = w.file.Close()
		w.closed = true
		w.file = nil
		w.buffer = nil
		return err
	}
	if w.fsyncOnClose {
		if err := w.file.Sync(); err != nil {
			_ = lockNDJSONFile(w.file, syscall.LOCK_UN)
			_ = w.file.Close()
			w.closed = true
			w.file = nil
			w.buffer = nil
			return err
		}
	}
	if err := lockNDJSONFile(w.file, syscall.LOCK_UN); err != nil {
		_ = w.file.Close()
		w.closed = true
		w.file = nil
		w.buffer = nil
		return err
	}
	if err := w.file.Close(); err != nil {
		w.closed = true
		w.file = nil
		w.buffer = nil
		return err
	}

	w.closed = true
	w.file = nil
	w.buffer = nil
	return nil
}

func (w *ndjsonWriter) flushBuffer() error {
	if w == nil || w.buffer == nil || w.pendingLines == 0 {
		return nil
	}
	if err := w.buffer.Flush(); err != nil {
		return err
	}
	w.pendingLines = 0
	return nil
}

func normalizeNDJSONFlushEvery(flushEvery int) int {
	if flushEvery <= 0 {
		return 1
	}
	return flushEvery
}

func lockNDJSONFile(file *os.File, mode int) error {
	if file == nil {
		return errors.New("ndjson writer file is nil")
	}

	fd, err := strconv.Atoi(fmt.Sprint(file.Fd()))
	if err != nil {
		return err
	}
	return syscall.Flock(fd, mode)
}
