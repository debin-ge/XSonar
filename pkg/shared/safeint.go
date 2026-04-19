package shared

import (
	"fmt"
	"strconv"
)

func Int32FromInt(value int) (int32, error) {
	var result int32
	if _, err := fmt.Sscan(strconv.Itoa(value), &result); err != nil {
		return 0, err
	}
	return result, nil
}
