package internal

import (
	"context"
	"reflect"
	"testing"
	"time"
)

func TestMemoryWorkerStoreListTaskSeenPostsReturnsExistingSubset(t *testing.T) {
	store := newMemoryWorkerStore()
	seenAt := time.Now().UTC()

	seen, err := store.RecordTaskSeenPost(context.Background(), "task_1", "post_1", "run_1", seenAt)
	if err != nil {
		t.Fatalf("seed post_1: %v", err)
	}
	if !seen {
		t.Fatalf("expected post_1 seed to be recorded")
	}
	seen, err = store.RecordTaskSeenPost(context.Background(), "task_1", "post_3", "run_1", seenAt)
	if err != nil {
		t.Fatalf("seed post_3: %v", err)
	}
	if !seen {
		t.Fatalf("expected post_3 seed to be recorded")
	}

	got, err := store.ListTaskSeenPosts(context.Background(), "task_1", []string{"post_1", "post_2", "post_3", "post_3"})
	if err != nil {
		t.Fatalf("ListTaskSeenPosts: %v", err)
	}

	want := map[string]bool{
		"post_1": true,
		"post_3": true,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("expected seen subset %v, got %v", want, got)
	}
}
