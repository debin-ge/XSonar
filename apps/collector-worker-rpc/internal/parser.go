package internal

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

type extractedPage struct {
	Posts      []extractedPost
	NextCursor string
}

type extractedPost struct {
	PostID     string
	RawPayload json.RawMessage
}

func extractPage(payload []byte) (extractedPage, error) {
	normalized, err := unwrapPagePayload(payload)
	if err != nil {
		return extractedPage{}, fmt.Errorf("decode page payload: %w", err)
	}

	if page, ok, err := extractTimelinePage(normalized); ok || err != nil {
		return page, err
	}

	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(normalized, &envelope); err != nil {
		return extractedPage{}, fmt.Errorf("decode page payload: %w", err)
	}

	tweetsPayload, ok := envelope["tweets"]
	if !ok {
		var nested struct {
			Data map[string]json.RawMessage `json:"data"`
		}
		if err := json.Unmarshal(normalized, &nested); err == nil && nested.Data != nil {
			tweetsPayload = nested.Data["tweets"]
		}
	}
	if len(tweetsPayload) == 0 {
		return extractedPage{}, errors.New("tweets payload is required")
	}

	var rawPosts []json.RawMessage
	if err := json.Unmarshal(tweetsPayload, &rawPosts); err != nil {
		return extractedPage{}, fmt.Errorf("decode tweets payload: %w", err)
	}

	posts := make([]extractedPost, 0, len(rawPosts))
	for _, rawPost := range rawPosts {
		postID, err := extractPostID(rawPost)
		if err != nil {
			return extractedPage{}, err
		}
		posts = append(posts, extractedPost{
			PostID:     postID,
			RawPayload: append(json.RawMessage(nil), rawPost...),
		})
	}

	return extractedPage{
		Posts:      posts,
		NextCursor: extractCursor(envelope),
	}, nil
}

func unwrapPagePayload(payload []byte) ([]byte, error) {
	current := bytes.TrimSpace(payload)
	for depth := 0; depth < 4; depth++ {
		if len(current) == 0 || current[0] != '"' {
			return current, nil
		}

		var decoded string
		if err := json.Unmarshal(current, &decoded); err != nil {
			return nil, err
		}
		current = bytes.TrimSpace([]byte(decoded))
	}

	return current, nil
}

func extractTimelinePage(payload []byte) (extractedPage, bool, error) {
	trimmed := bytes.TrimSpace(payload)
	if len(trimmed) == 0 {
		return extractedPage{}, false, nil
	}
	if !bytes.Contains(trimmed, []byte("search_by_raw_query")) &&
		!bytes.Contains(trimmed, []byte("tweet_results")) &&
		!bytes.Contains(trimmed, []byte("TimelineTimelineCursor")) {
		return extractedPage{}, false, nil
	}

	posts := make([]extractedPost, 0, 8)
	seen := make(map[string]struct{})
	cursor, err := walkTimelinePayload(json.RawMessage(trimmed), &posts, seen, "")
	if err != nil {
		return extractedPage{}, true, err
	}

	return extractedPage{
		Posts:      posts,
		NextCursor: cursor,
	}, true, nil
}

func walkTimelinePayload(raw json.RawMessage, posts *[]extractedPost, seen map[string]struct{}, bottomCursor string) (string, error) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || string(raw) == "null" {
		return bottomCursor, nil
	}

	switch raw[0] {
	case '{':
		object, ok := rawJSONObject(raw)
		if !ok {
			return bottomCursor, nil
		}

		if cursor := extractBottomCursor(object); cursor != "" {
			bottomCursor = cursor
		}
		if tweetRaw := extractTimelineTweetRaw(object); len(tweetRaw) != 0 {
			postID, err := extractPostID(tweetRaw)
			if err == nil {
				if _, exists := seen[postID]; !exists {
					seen[postID] = struct{}{}
					*posts = append(*posts, extractedPost{
						PostID:     postID,
						RawPayload: append(json.RawMessage(nil), tweetRaw...),
					})
				}
			}
		}

		for _, value := range object {
			cursor, err := walkTimelinePayload(value, posts, seen, bottomCursor)
			if err != nil {
				return bottomCursor, err
			}
			if cursor != "" {
				bottomCursor = cursor
			}
		}
		return bottomCursor, nil
	case '[':
		var items []json.RawMessage
		if err := json.Unmarshal(raw, &items); err != nil {
			return bottomCursor, fmt.Errorf("decode timeline array: %w", err)
		}
		for _, item := range items {
			cursor, err := walkTimelinePayload(item, posts, seen, bottomCursor)
			if err != nil {
				return bottomCursor, err
			}
			if cursor != "" {
				bottomCursor = cursor
			}
		}
	}

	return bottomCursor, nil
}

func extractBottomCursor(object map[string]json.RawMessage) string {
	var cursorType string
	if err := json.Unmarshal(object["cursorType"], &cursorType); err != nil || !strings.EqualFold(cursorType, "Bottom") {
		return ""
	}

	var value string
	if err := json.Unmarshal(object["value"], &value); err != nil {
		return ""
	}
	return strings.TrimSpace(value)
}

func extractTimelineTweetRaw(object map[string]json.RawMessage) json.RawMessage {
	if tweetResults := object["tweet_results"]; len(tweetResults) != 0 {
		return unwrapTimelineTweetRaw(tweetResults)
	}
	if looksLikeTimelineTweetObject(object) {
		body, err := json.Marshal(object)
		if err != nil {
			return nil
		}
		return cloneRawJSON(body)
	}
	return nil
}

func unwrapTimelineTweetRaw(raw json.RawMessage) json.RawMessage {
	current := bytes.TrimSpace(raw)
	for depth := 0; depth < 6 && len(current) != 0; depth++ {
		object, ok := rawJSONObject(current)
		if !ok {
			return cloneRawJSON(current)
		}
		if result := object["result"]; len(result) != 0 {
			current = bytes.TrimSpace(result)
			continue
		}
		if tweet := object["tweet"]; len(tweet) != 0 {
			current = bytes.TrimSpace(tweet)
			continue
		}
		return cloneRawJSON(current)
	}
	return cloneRawJSON(current)
}

func looksLikeTimelineTweetObject(object map[string]json.RawMessage) bool {
	if len(object["rest_id"]) == 0 {
		return false
	}

	var typename string
	if err := json.Unmarshal(object["__typename"], &typename); err == nil && strings.Contains(strings.ToLower(typename), "tweet") {
		return true
	}

	return len(object["legacy"]) != 0 || len(object["core"]) != 0 || len(object["views"]) != 0
}

func rawJSONObject(body []byte) (map[string]json.RawMessage, bool) {
	var payload map[string]json.RawMessage
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, false
	}
	return payload, true
}

func cloneRawJSON(body []byte) json.RawMessage {
	if len(body) == 0 {
		return nil
	}
	cloned := make([]byte, len(body))
	copy(cloned, body)
	return json.RawMessage(cloned)
}

func extractPostID(raw json.RawMessage) (string, error) {
	var item map[string]json.RawMessage
	if err := json.Unmarshal(raw, &item); err != nil {
		return "", fmt.Errorf("decode post payload: %w", err)
	}

	for _, key := range []string{"id", "tweet_id", "tweetId", "rest_id"} {
		if payload := item[key]; len(payload) != 0 {
			var value string
			if err := json.Unmarshal(payload, &value); err == nil && value != "" {
				return value, nil
			}
		}
	}

	return "", errors.New("post_id is required")
}

func extractCursor(envelope map[string]json.RawMessage) string {
	for _, key := range []string{"next_cursor", "cursor"} {
		if payload := envelope[key]; len(payload) != 0 {
			var value string
			if err := json.Unmarshal(payload, &value); err == nil {
				return value
			}
		}
	}

	if payload := envelope["data"]; len(payload) != 0 {
		var nested map[string]json.RawMessage
		if err := json.Unmarshal(payload, &nested); err == nil {
			for _, key := range []string{"next_cursor", "cursor"} {
				if cursorPayload := nested[key]; len(cursorPayload) != 0 {
					var value string
					if err := json.Unmarshal(cursorPayload, &value); err == nil {
						return value
					}
				}
			}
		}
	}

	return ""
}
