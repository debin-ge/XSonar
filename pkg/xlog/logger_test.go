package xlog

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoggerRotatesFileWhenMaxSizeExceeded(t *testing.T) {
	logDir := t.TempDir()
	logger, err := NewWithOptions("gateway-api", logDir, Options{
		Stdout:         &bytes.Buffer{},
		MaxSizeBytes:   220,
		MaxBackupFiles: 2,
	})
	if err != nil {
		t.Fatalf("new logger: %v", err)
	}
	defer func() { _ = logger.Close() }()

	for i := 0; i < 6; i++ {
		logger.Info("gateway request completed", map[string]any{
			"request_id": "req-rotation-test",
			"payload":    strings.Repeat("x", 80),
		})
	}

	files, err := os.ReadDir(logDir)
	if err != nil {
		t.Fatalf("read log dir: %v", err)
	}

	currentFound := false
	rotatedCount := 0
	for _, file := range files {
		switch {
		case file.Name() == "gateway-api.log":
			currentFound = true
		case strings.HasPrefix(file.Name(), "gateway-api-") && strings.HasSuffix(file.Name(), ".log"):
			rotatedCount++
		}
	}

	if !currentFound {
		t.Fatal("expected current log file to exist")
	}
	if rotatedCount == 0 {
		t.Fatalf("expected at least one rotated log file, got %d", rotatedCount)
	}
	if rotatedCount > 2 {
		t.Fatalf("expected at most 2 rotated log files, got %d", rotatedCount)
	}
}

func TestLoggerPrunesRotatedBackups(t *testing.T) {
	logDir := t.TempDir()
	logger, err := NewWithOptions("provider-rpc", logDir, Options{
		Stdout:         &bytes.Buffer{},
		MaxSizeBytes:   180,
		MaxBackupFiles: 1,
	})
	if err != nil {
		t.Fatalf("new logger: %v", err)
	}
	defer func() { _ = logger.Close() }()

	for i := 0; i < 10; i++ {
		logger.Error("provider request executed", map[string]any{
			"request_id": "req-provider-rotation",
			"payload":    strings.Repeat("y", 90),
		})
	}

	matches, err := filepath.Glob(filepath.Join(logDir, "provider-rpc-*.log"))
	if err != nil {
		t.Fatalf("glob rotated logs: %v", err)
	}
	if len(matches) != 1 {
		t.Fatalf("expected exactly 1 rotated log file, got %d", len(matches))
	}

	currentPath := filepath.Join(logDir, "provider-rpc.log")
	if _, err := os.Stat(currentPath); err != nil {
		t.Fatalf("expected current log file, got error: %v", err)
	}
}

func TestLoggerCreatesRestrictedDirectoryAndFilePermissions(t *testing.T) {
	rootDir := t.TempDir()
	logDir := filepath.Join(rootDir, "logs")

	logger, err := NewWithOptions("gateway-api", logDir, Options{
		Stdout: &bytes.Buffer{},
	})
	if err != nil {
		t.Fatalf("new logger: %v", err)
	}
	defer func() { _ = logger.Close() }()

	dirInfo, err := os.Stat(logDir)
	if err != nil {
		t.Fatalf("stat log dir: %v", err)
	}
	if got := dirInfo.Mode().Perm(); got != 0o750 {
		t.Fatalf("expected log dir mode 0750, got %04o", got)
	}

	fileInfo, err := os.Stat(filepath.Join(logDir, "gateway-api.log"))
	if err != nil {
		t.Fatalf("stat log file: %v", err)
	}
	if got := fileInfo.Mode().Perm(); got != 0o600 {
		t.Fatalf("expected log file mode 0600, got %04o", got)
	}
}
