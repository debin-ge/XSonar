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

func TestNDJSONWriterFlushesOnConfiguredRecordBoundary(t *testing.T) {
	root := t.TempDir()
	finalPath := filepath.Join(root, "task_1", "task_1.ndjson")

	writer, err := newNDJSONWriter(finalPath, 2, true)
	if err != nil {
		t.Fatalf("newNDJSONWriter returned error: %v", err)
	}
	defer func() { _ = writer.Abort() }()

	if err := writer.Append(map[string]any{"task_id": "task_1", "post_id": "post_1"}); err != nil {
		t.Fatalf("first Append returned error: %v", err)
	}

	data, err := os.ReadFile(writer.partPath)
	if err != nil {
		t.Fatalf("ReadFile after first append returned error: %v", err)
	}
	if len(data) != 0 {
		t.Fatalf("expected writer to keep first record buffered, got %q", string(data))
	}

	if err := writer.Append(map[string]any{"task_id": "task_1", "post_id": "post_2"}); err != nil {
		t.Fatalf("second Append returned error: %v", err)
	}

	data, err = os.ReadFile(writer.partPath)
	if err != nil {
		t.Fatalf("ReadFile after second append returned error: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected flush boundary to publish two lines, got %d", len(lines))
	}
}

func TestNDJSONWriterBeginBatchAppendDefersAutoFlushUntilExplicitFlush(t *testing.T) {
	root := t.TempDir()
	finalPath := filepath.Join(root, "task_1", "task_1.ndjson")

	writer, err := newNDJSONWriter(finalPath, 1, true)
	if err != nil {
		t.Fatalf("newNDJSONWriter returned error: %v", err)
	}
	defer func() { _ = writer.Abort() }()

	restoreBatch := writer.beginBatchAppend(2)
	if err := writer.Append(map[string]any{"task_id": "task_1", "post_id": "post_1"}); err != nil {
		t.Fatalf("first Append returned error: %v", err)
	}
	if err := writer.Append(map[string]any{"task_id": "task_1", "post_id": "post_2"}); err != nil {
		t.Fatalf("second Append returned error: %v", err)
	}

	data, err := os.ReadFile(writer.partPath)
	if err != nil {
		t.Fatalf("ReadFile before explicit Flush returned error: %v", err)
	}
	if len(data) != 0 {
		t.Fatalf("expected batch append to keep records buffered, got %q", string(data))
	}

	restoreBatch()
	if err := writer.Flush(); err != nil {
		t.Fatalf("Flush returned error: %v", err)
	}

	data, err = os.ReadFile(writer.partPath)
	if err != nil {
		t.Fatalf("ReadFile after explicit Flush returned error: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected explicit Flush to publish two buffered lines, got %d", len(lines))
	}

	if err := writer.Append(map[string]any{"task_id": "task_1", "post_id": "post_3"}); err != nil {
		t.Fatalf("third Append returned error: %v", err)
	}

	data, err = os.ReadFile(writer.partPath)
	if err != nil {
		t.Fatalf("ReadFile after restored boundary returned error: %v", err)
	}
	lines = strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 3 {
		t.Fatalf("expected restored flush boundary to auto-publish third line, got %d", len(lines))
	}
}

func TestNDJSONWriterAppendAndFlushPublishesRecordBeforeClose(t *testing.T) {
	root := t.TempDir()
	finalPath := filepath.Join(root, "task_1", "task_1.ndjson")

	writer, err := newNDJSONWriter(finalPath, 100, true)
	if err != nil {
		t.Fatalf("newNDJSONWriter returned error: %v", err)
	}
	defer func() { _ = writer.Abort() }()

	if err := writer.AppendAndFlush(map[string]any{"task_id": "task_1", "post_id": "post_1"}); err != nil {
		t.Fatalf("AppendAndFlush returned error: %v", err)
	}

	data, err := os.ReadFile(writer.partPath)
	if err != nil {
		t.Fatalf("ReadFile after AppendAndFlush returned error: %v", err)
	}
	if !strings.Contains(string(data), `"post_id":"post_1"`) {
		t.Fatalf("expected flushed part file to contain appended record, got %q", string(data))
	}
}

func TestNDJSONWriterUsesRestrictedPermissions(t *testing.T) {
	root := t.TempDir()
	finalPath := filepath.Join(root, "task_1", "task_1.ndjson")

	writer, err := newNDJSONWriter(finalPath, 100, true)
	if err != nil {
		t.Fatalf("newNDJSONWriter returned error: %v", err)
	}
	defer func() { _ = writer.Abort() }()

	dirInfo, err := os.Stat(filepath.Dir(finalPath))
	if err != nil {
		t.Fatalf("stat writer dir: %v", err)
	}
	if got := dirInfo.Mode().Perm(); got != 0o750 {
		t.Fatalf("expected dir mode 0750, got %04o", got)
	}

	fileInfo, err := os.Stat(writer.partPath)
	if err != nil {
		t.Fatalf("stat part file: %v", err)
	}
	if got := fileInfo.Mode().Perm(); got != 0o600 {
		t.Fatalf("expected part file mode 0600, got %04o", got)
	}
}
