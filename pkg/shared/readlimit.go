package shared

import (
	"fmt"
	"io"
)

func ReadAllWithLimit(reader io.Reader, maxBytes int64) ([]byte, error) {
	if maxBytes <= 0 {
		return io.ReadAll(reader)
	}

	limitedReader := io.LimitReader(reader, maxBytes+1)
	body, err := io.ReadAll(limitedReader)
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > maxBytes {
		return nil, fmt.Errorf("response body exceeds %d bytes", maxBytes)
	}
	return body, nil
}
