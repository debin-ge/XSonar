package xlog

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

type Logger struct {
	service        string
	stdout         io.Writer
	file           *os.File
	filePath       string
	maxSizeBytes   int64
	maxBackupFiles int
	mu             sync.Mutex
}

type Options struct {
	Stdout         io.Writer
	MaxSizeBytes   int64
	MaxBackupFiles int
}

func New(service, logDir string) (*Logger, error) {
	return NewWithOptions(service, logDir, Options{})
}

func NewWithOptions(service, logDir string, options Options) (*Logger, error) {
	options = normalizeOptions(options)
	if logDir == "" {
		return NewWithWriter(service, options.Stdout), nil
	}

	if err := os.MkdirAll(logDir, 0o755); err != nil {
		return NewWithWriter(service, options.Stdout), fmt.Errorf("create log dir: %w", err)
	}

	logPath := filepath.Join(logDir, service+".log")
	file, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return NewWithWriter(service, options.Stdout), fmt.Errorf("open log file: %w", err)
	}

	return &Logger{
		service:        service,
		stdout:         options.Stdout,
		file:           file,
		filePath:       logPath,
		maxSizeBytes:   options.MaxSizeBytes,
		maxBackupFiles: options.MaxBackupFiles,
	}, nil
}

func NewStdout(service string) *Logger {
	return NewWithWriter(service, os.Stdout)
}

func NewWithWriter(service string, writer io.Writer) *Logger {
	if writer == nil {
		writer = io.Discard
	}
	return &Logger{
		service: service,
		stdout:  writer,
	}
}

func (l *Logger) Close() error {
	if l == nil || l.file == nil {
		return nil
	}

	return l.file.Close()
}

func (l *Logger) Info(message string, fields map[string]any) {
	l.log("info", message, fields)
}

func (l *Logger) Error(message string, fields map[string]any) {
	l.log("error", message, fields)
}

func (l *Logger) log(level, message string, fields map[string]any) {
	if l == nil {
		return
	}

	record := map[string]any{
		"timestamp": time.Now().Format(time.RFC3339Nano),
		"service":   l.service,
		"level":     level,
		"message":   message,
	}

	for key, value := range fields {
		record[key] = value
	}

	payload, err := json.Marshal(record)
	if err != nil {
		payload = []byte(fmt.Sprintf(`{"timestamp":"%s","service":"%s","level":"error","message":"marshal log record failed","reason":%q}`,
			time.Now().Format(time.RFC3339Nano), l.service, err.Error()))
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	line := append(payload, '\n')
	if l.stdout != nil {
		_, _ = l.stdout.Write(line)
	}
	if l.file == nil {
		return
	}
	if err := l.rotateIfNeeded(int64(len(line))); err != nil {
		l.writeInternalErrorLocked(fmt.Errorf("rotate log file: %w", err))
	}
	if _, err := l.file.Write(line); err != nil {
		l.writeInternalErrorLocked(fmt.Errorf("write log file: %w", err))
	}
}

func (l *Logger) rotateIfNeeded(incomingBytes int64) error {
	if l.file == nil || l.maxSizeBytes <= 0 {
		return nil
	}

	info, err := l.file.Stat()
	if err != nil {
		return err
	}
	if info.Size()+incomingBytes <= l.maxSizeBytes {
		return nil
	}

	if err := l.file.Close(); err != nil {
		return err
	}

	rotatedPath := filepath.Join(filepath.Dir(l.filePath), l.rotatedFilename())
	if err := os.Rename(l.filePath, rotatedPath); err != nil {
		return err
	}

	file, err := os.OpenFile(l.filePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	l.file = file

	return l.pruneRotatedFiles()
}

func (l *Logger) rotatedFilename() string {
	return fmt.Sprintf("%s-%s.log", l.service, time.Now().UTC().Format("20060102T150405.000000000Z"))
}

func (l *Logger) pruneRotatedFiles() error {
	pattern := filepath.Join(filepath.Dir(l.filePath), l.service+"-*.log")
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return err
	}
	if len(matches) <= l.maxBackupFiles {
		return nil
	}

	sort.Strings(matches)
	for _, path := range matches[:len(matches)-l.maxBackupFiles] {
		if strings.TrimSpace(path) == "" {
			continue
		}
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

func (l *Logger) writeInternalErrorLocked(err error) {
	if l.stdout == nil || err == nil {
		return
	}
	payload, marshalErr := json.Marshal(map[string]any{
		"timestamp": time.Now().UTC().Format(time.RFC3339Nano),
		"service":   l.service,
		"level":     "error",
		"message":   "logger internal failure",
		"reason":    err.Error(),
	})
	if marshalErr != nil {
		return
	}
	_, _ = l.stdout.Write(append(payload, '\n'))
}

func normalizeOptions(options Options) Options {
	if options.Stdout == nil {
		options.Stdout = os.Stdout
	}
	if options.MaxSizeBytes <= 0 {
		options.MaxSizeBytes = 20 * 1024 * 1024
	}
	if options.MaxBackupFiles < 0 {
		options.MaxBackupFiles = 0
	}
	return options
}
