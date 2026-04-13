package internal

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"syscall"
)

type ndjsonWriter struct {
	partPath     string
	finalPath    string
	file         *os.File
	fsyncOnClose bool
	closed       bool
	committed    bool
}

func newNDJSONWriter(finalPath string, flushEvery int, fsyncOnClose bool) (*ndjsonWriter, error) {
	_ = flushEvery

	finalPath = filepath.Clean(finalPath)
	partPath := finalPath + ".part"
	if err := os.MkdirAll(filepath.Dir(finalPath), 0o755); err != nil {
		return nil, err
	}

	file, err := os.OpenFile(partPath, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX); err != nil {
		_ = file.Close()
		return nil, err
	}

	return &ndjsonWriter{
		partPath:     partPath,
		finalPath:    finalPath,
		file:         file,
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
	_, err = w.file.Write(line)
	return err
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

	if w.fsyncOnClose {
		if err := w.file.Sync(); err != nil {
			_ = syscall.Flock(int(w.file.Fd()), syscall.LOCK_UN)
			_ = w.file.Close()
			w.closed = true
			return err
		}
	}
	if err := syscall.Flock(int(w.file.Fd()), syscall.LOCK_UN); err != nil {
		_ = w.file.Close()
		w.closed = true
		return err
	}
	if err := w.file.Close(); err != nil {
		w.closed = true
		return err
	}

	w.closed = true
	w.file = nil
	return nil
}
