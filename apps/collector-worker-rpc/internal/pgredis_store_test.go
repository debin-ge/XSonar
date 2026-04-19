package internal

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestNormalizeUsageMonthFallsBackToSeenMonthStart(t *testing.T) {
	seenAt := time.Date(2026, 4, 13, 12, 30, 0, 0, time.UTC)
	if got := normalizeUsageMonth("", seenAt); got != "2026-04-01" {
		t.Fatalf("expected usage month 2026-04-01, got %q", got)
	}
}

func TestNormalizeUsageMonthKeepsExplicitValue(t *testing.T) {
	if got := normalizeUsageMonth("2026-04-01", time.Now().UTC()); got != "2026-04-01" {
		t.Fatalf("expected explicit usage month to be preserved, got %q", got)
	}
}

func TestPGRedisWorkerStoreListTaskSeenPostsScansArrayResult(t *testing.T) {
	db := &stubWorkerDB{
		row: stubWorkerRow{
			scanFn: func(dest ...any) error {
				got, ok := dest[0].(*[]string)
				if !ok {
					t.Fatalf("expected first scan destination to be *[]string, got %T", dest[0])
				}
				*got = []string{"post_1", "post_3"}
				return nil
			},
		},
	}
	store := &pgRedisWorkerStore{db: db}

	got, err := store.ListTaskSeenPosts(context.Background(), " task_1 ", []string{"post_1", " post_2 ", "post_3", "post_3", "   "})
	if err != nil {
		t.Fatalf("ListTaskSeenPosts: %v", err)
	}

	want := map[string]bool{
		"post_1": true,
		"post_3": true,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("expected seen posts %v, got %v", want, got)
	}
	if !strings.Contains(db.lastQuery, "ANY($2)") {
		t.Fatalf("expected query to use ANY($2), got %q", db.lastQuery)
	}
	if len(db.lastArgs) != 2 {
		t.Fatalf("expected 2 query args, got %d", len(db.lastArgs))
	}
	if db.lastArgs[0] != "task_1" {
		t.Fatalf("expected task ID to be trimmed, got %#v", db.lastArgs[0])
	}
	posts, ok := db.lastArgs[1].([]string)
	if !ok {
		t.Fatalf("expected post IDs arg to be []string, got %T", db.lastArgs[1])
	}
	if !reflect.DeepEqual(posts, []string{"post_1", "post_2", "post_3"}) {
		t.Fatalf("expected deduped trimmed post IDs, got %v", posts)
	}
}

type stubWorkerDB struct {
	lastQuery string
	lastArgs  []any
	row       workerRow
}

func (db *stubWorkerDB) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	panic("unexpected Exec call")
}

func (db *stubWorkerDB) QueryRow(_ context.Context, query string, args ...any) workerRow {
	db.lastQuery = query
	db.lastArgs = append([]any(nil), args...)
	return db.row
}

type stubWorkerRow struct {
	scanFn func(dest ...any) error
}

func (r stubWorkerRow) Scan(dest ...any) error {
	if r.scanFn != nil {
		return r.scanFn(dest...)
	}
	return nil
}
