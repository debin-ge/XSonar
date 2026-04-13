package internal

import (
	"testing"
)

func TestExtractPageParsesPostIDAndCursor(t *testing.T) {
	payload := []byte(`{
		"tweets": [
			{
				"id": "1940603123074228282",
				"text": "openai update",
				"bookmark": 12,
				"views": 345
			}
		],
		"next_cursor": "cursor-123"
	}`)

	page, err := extractPage(payload)
	if err != nil {
		t.Fatalf("extractPage returned error: %v", err)
	}
	if page.NextCursor != "cursor-123" {
		t.Fatalf("expected next cursor cursor-123, got %q", page.NextCursor)
	}
	if len(page.Posts) != 1 {
		t.Fatalf("expected 1 post, got %d", len(page.Posts))
	}
	if page.Posts[0].PostID != "1940603123074228282" {
		t.Fatalf("expected post_id 1940603123074228282, got %q", page.Posts[0].PostID)
	}
	if len(page.Posts[0].RawPayload) == 0 {
		t.Fatal("expected raw payload to be preserved")
	}
}

func TestExtractPageAllowsMissingBookmarkAndViews(t *testing.T) {
	payload := []byte(`{
		"tweets": [
			{
				"id": "1940603123074228282",
				"text": "openai update"
			}
		]
	}`)

	page, err := extractPage(payload)
	if err != nil {
		t.Fatalf("extractPage returned error: %v", err)
	}
	if len(page.Posts) != 1 {
		t.Fatalf("expected 1 post, got %d", len(page.Posts))
	}
	if page.Posts[0].PostID != "1940603123074228282" {
		t.Fatalf("expected post_id 1940603123074228282, got %q", page.Posts[0].PostID)
	}
}

func TestExtractPageRejectsMalformedPayload(t *testing.T) {
	if _, err := extractPage([]byte(`{"tweets":[`)); err == nil {
		t.Fatal("expected malformed payload to return an error")
	}
}
