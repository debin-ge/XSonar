package internal

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNDJSONWriterWritesPartUntilCommit(t *testing.T) {
	root := t.TempDir()
	finalPath := filepath.Join(root, "task_1", "task_1.ndjson")

	writer, err := newNDJSONWriter(finalPath, 100, true)
	if err != nil {
		t.Fatalf("newNDJSONWriter returned error: %v", err)
	}
	defer func() { _ = writer.Abort() }()

	if err := writer.Append(map[string]any{"task_id": "task_1", "post_id": "post_1"}); err != nil {
		t.Fatalf("Append returned error: %v", err)
	}

	if _, err := os.Stat(finalPath); err == nil {
		t.Fatal("final file should not exist before commit")
	}
	if _, err := os.Stat(writer.partPath); err != nil {
		t.Fatalf("expected .part file to exist, got %v", err)
	}
}

func TestNDJSONWriterCommitPublishesFinalFile(t *testing.T) {
	root := t.TempDir()
	finalPath := filepath.Join(root, "task_1", "task_1.ndjson")

	writer, err := newNDJSONWriter(finalPath, 100, true)
	if err != nil {
		t.Fatalf("newNDJSONWriter returned error: %v", err)
	}

	if err := writer.Append(map[string]any{"task_id": "task_1", "post_id": "post_1"}); err != nil {
		t.Fatalf("Append returned error: %v", err)
	}
	if err := writer.Commit(); err != nil {
		t.Fatalf("Commit returned error: %v", err)
	}

	if _, err := os.Stat(writer.partPath); !os.IsNotExist(err) {
		t.Fatalf("expected .part file to be gone after commit, got %v", err)
	}
	data, err := os.ReadFile(finalPath)
	if err != nil {
		t.Fatalf("ReadFile returned error: %v", err)
	}
	if !strings.HasSuffix(string(data), "\n") {
		t.Fatalf("expected final NDJSON file to end with newline, got %q", string(data))
	}
}

func TestNDJSONWriterResumeModeAppendsExistingPart(t *testing.T) {
	root := t.TempDir()
	finalPath := filepath.Join(root, "task_1", "task_1.ndjson")

	first, err := newNDJSONWriter(finalPath, 100, true)
	if err != nil {
		t.Fatalf("first newNDJSONWriter returned error: %v", err)
	}
	if err := first.Append(map[string]any{"task_id": "task_1", "post_id": "post_1"}); err != nil {
		t.Fatalf("first Append returned error: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("first Close returned error: %v", err)
	}

	second, err := newNDJSONWriter(finalPath, 100, true)
	if err != nil {
		t.Fatalf("second newNDJSONWriter returned error: %v", err)
	}
	if err := second.Append(map[string]any{"task_id": "task_1", "post_id": "post_2"}); err != nil {
		t.Fatalf("second Append returned error: %v", err)
	}
	if err := second.Commit(); err != nil {
		t.Fatalf("second Commit returned error: %v", err)
	}

	data, err := os.ReadFile(finalPath)
	if err != nil {
		t.Fatalf("ReadFile returned error: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 NDJSON lines after resume append, got %d", len(lines))
	}
}
