package internal

import (
	"encoding/json"
	"errors"
	"fmt"
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
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return extractedPage{}, fmt.Errorf("decode page payload: %w", err)
	}

	tweetsPayload, ok := envelope["tweets"]
	if !ok {
		var nested struct {
			Data map[string]json.RawMessage `json:"data"`
		}
		if err := json.Unmarshal(payload, &nested); err == nil && nested.Data != nil {
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
