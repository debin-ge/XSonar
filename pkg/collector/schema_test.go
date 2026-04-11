package collector

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestSchemaSQLIncludesDueTaskIndex(t *testing.T) {
	t.Parallel()

	const want = "CREATE INDEX IF NOT EXISTS idx_collector_tasks_status_next_run_at\n    ON collector.tasks (status, next_run_at);"
	if !strings.Contains(SchemaSQL, want) {
		t.Fatalf("SchemaSQL missing due-task index %q", want)
	}
}

func TestSchemaSQLMatchesMigration(t *testing.T) {
	t.Parallel()

	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller(0) failed")
	}

	migrationPath := filepath.Clean(filepath.Join(filepath.Dir(filename), "..", "..", "migrations", "collector", "0001_init.sql"))
	gotBytes, err := os.ReadFile(migrationPath)
	if err != nil {
		t.Fatalf("read migration %s: %v", migrationPath, err)
	}

	got := strings.TrimSpace(string(gotBytes))
	want := strings.TrimSpace(SchemaSQL)
	if got != want {
		t.Fatalf("SchemaSQL does not match %s", migrationPath)
	}
}
