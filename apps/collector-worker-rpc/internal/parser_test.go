package internal

import (
	"encoding/json"
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

func TestExtractPageParsesStringEncodedTimelinePayload(t *testing.T) {
	payload := mustEncodeJSONString(t, `{
		"search_by_raw_query": {
			"search_timeline": {
				"timeline": {
					"instructions": [
						{
							"type": "TimelineAddEntries",
							"entries": [
								{
									"entryId": "tweet-1940603123074228282",
									"content": {
										"__typename": "TimelineTimelineItem",
										"itemContent": {
											"itemType": "TimelineTweet",
											"tweet_results": {
												"result": {
													"__typename": "Tweet",
													"rest_id": "1940603123074228282",
													"legacy": {
														"full_text": "openai update"
													}
												}
											}
										}
									}
								},
								{
									"entryId": "cursor-bottom-0",
									"content": {
										"__typename": "TimelineTimelineCursor",
										"cursorType": "Bottom",
										"value": "cursor-bottom-123"
									}
								}
							]
						}
					]
				}
			}
		}
	}`)

	page, err := extractPage(payload)
	if err != nil {
		t.Fatalf("extractPage returned error: %v", err)
	}
	if page.NextCursor != "cursor-bottom-123" {
		t.Fatalf("expected next cursor cursor-bottom-123, got %q", page.NextCursor)
	}
	if len(page.Posts) != 1 {
		t.Fatalf("expected 1 post, got %d", len(page.Posts))
	}
	if page.Posts[0].PostID != "1940603123074228282" {
		t.Fatalf("expected post_id 1940603123074228282, got %q", page.Posts[0].PostID)
	}
}

func TestExtractPageAllowsStringEncodedTimelineWithoutTweets(t *testing.T) {
	payload := mustEncodeJSONString(t, `{
		"search_by_raw_query": {
			"search_timeline": {
				"timeline": {
					"instructions": [
						{
							"type": "TimelineAddEntries",
							"entries": [
								{
									"entryId": "cursor-top-1",
									"content": {
										"__typename": "TimelineTimelineCursor",
										"cursorType": "Top",
										"value": "cursor-top-123"
									}
								},
								{
									"entryId": "cursor-bottom-0",
									"content": {
										"__typename": "TimelineTimelineCursor",
										"cursorType": "Bottom",
										"value": "cursor-bottom-123"
									}
								}
							]
						}
					]
				}
			}
		}
	}`)

	page, err := extractPage(payload)
	if err != nil {
		t.Fatalf("extractPage returned error: %v", err)
	}
	if page.NextCursor != "cursor-bottom-123" {
		t.Fatalf("expected next cursor cursor-bottom-123, got %q", page.NextCursor)
	}
	if len(page.Posts) != 0 {
		t.Fatalf("expected 0 posts, got %d", len(page.Posts))
	}
}

func TestExtractPageRejectsMalformedPayload(t *testing.T) {
	if _, err := extractPage([]byte(`{"tweets":[`)); err == nil {
		t.Fatal("expected malformed payload to return an error")
	}
}

func mustEncodeJSONString(t *testing.T, raw string) []byte {
	t.Helper()

	body, err := json.Marshal(raw)
	if err != nil {
		t.Fatalf("marshal JSON string: %v", err)
	}
	return body
}
