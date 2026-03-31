package shared

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unicode"
)

var durationType = reflect.TypeOf(time.Duration(0))

var knownInitialisms = []string{
	"HTTPS",
	"HTTP",
	"JSON",
	"JWT",
	"TTL",
	"RPC",
	"API",
	"URL",
	"URI",
	"DSN",
	"QPS",
	"ID",
	"IP",
}

func ApplyEnvOverrides(prefix string, cfg any) error {
	value := reflect.ValueOf(cfg)
	if !value.IsValid() || value.Kind() != reflect.Pointer || value.IsNil() {
		return fmt.Errorf("apply env overrides requires a non-nil pointer")
	}
	return applyEnvValue(strings.ToUpper(strings.TrimSpace(prefix)), value.Elem(), nil)
}

func ApplyEnvOverridesWithPrefixes(cfg any, prefixes ...string) error {
	for _, prefix := range prefixes {
		if err := ApplyEnvOverrides(prefix, cfg); err != nil {
			return err
		}
	}
	return nil
}

func applyEnvValue(prefix string, value reflect.Value, path []string) error {
	if !value.IsValid() {
		return nil
	}

	if value.Kind() == reflect.Pointer {
		if value.IsNil() {
			if !value.CanSet() {
				return nil
			}
			value.Set(reflect.New(value.Type().Elem()))
		}
		return applyEnvValue(prefix, value.Elem(), path)
	}

	if value.Type() == durationType {
		return applyScalarOverride(prefix, value, path)
	}

	switch value.Kind() {
	case reflect.Struct:
		typ := value.Type()
		for idx := 0; idx < value.NumField(); idx++ {
			field := typ.Field(idx)
			if field.PkgPath != "" {
				continue
			}

			nextPath := path
			if !field.Anonymous {
				nextPath = append(clonePath(path), field.Name)
			}
			if err := applyEnvValue(prefix, value.Field(idx), nextPath); err != nil {
				return err
			}
		}
	case reflect.String, reflect.Bool,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64,
		reflect.Slice:
		return applyScalarOverride(prefix, value, path)
	}

	return nil
}

func applyScalarOverride(prefix string, value reflect.Value, path []string) error {
	if !value.CanSet() || len(path) == 0 {
		return nil
	}

	envKey := envKey(prefix, path)
	rawValue, exists := os.LookupEnv(envKey)
	if !exists {
		return nil
	}

	trimmed := strings.TrimSpace(rawValue)
	if trimmed == "" {
		return nil
	}

	switch {
	case value.Type() == durationType:
		parsed, err := parseDuration(trimmed)
		if err != nil {
			return fmt.Errorf("%s: %w", envKey, err)
		}
		value.SetInt(int64(parsed))
	case value.Kind() == reflect.String:
		value.SetString(trimmed)
	case value.Kind() == reflect.Bool:
		parsed, err := strconv.ParseBool(trimmed)
		if err != nil {
			return fmt.Errorf("%s: parse bool: %w", envKey, err)
		}
		value.SetBool(parsed)
	case value.Kind() >= reflect.Int && value.Kind() <= reflect.Int64:
		parsed, err := strconv.ParseInt(trimmed, 10, value.Type().Bits())
		if err != nil {
			return fmt.Errorf("%s: parse int: %w", envKey, err)
		}
		value.SetInt(parsed)
	case value.Kind() >= reflect.Uint && value.Kind() <= reflect.Uint64:
		parsed, err := strconv.ParseUint(trimmed, 10, value.Type().Bits())
		if err != nil {
			return fmt.Errorf("%s: parse uint: %w", envKey, err)
		}
		value.SetUint(parsed)
	case value.Kind() == reflect.Float32 || value.Kind() == reflect.Float64:
		parsed, err := strconv.ParseFloat(trimmed, value.Type().Bits())
		if err != nil {
			return fmt.Errorf("%s: parse float: %w", envKey, err)
		}
		value.SetFloat(parsed)
	case value.Kind() == reflect.Slice:
		parsed, err := parseSlice(trimmed, value.Type())
		if err != nil {
			return fmt.Errorf("%s: %w", envKey, err)
		}
		value.Set(parsed)
	}

	return nil
}

func parseDuration(raw string) (time.Duration, error) {
	if parsed, err := time.ParseDuration(raw); err == nil {
		return parsed, nil
	}
	if parsed, err := strconv.ParseInt(raw, 10, 64); err == nil {
		return time.Duration(parsed), nil
	}
	return 0, fmt.Errorf("parse duration %q", raw)
}

func parseSlice(raw string, sliceType reflect.Type) (reflect.Value, error) {
	target := reflect.New(sliceType)
	if strings.HasPrefix(raw, "[") {
		if err := json.Unmarshal([]byte(raw), target.Interface()); err == nil {
			return target.Elem(), nil
		}
	}

	if sliceType.Elem().Kind() != reflect.String {
		return reflect.Value{}, fmt.Errorf("unsupported slice override for %s, use JSON array", sliceType.String())
	}

	items := strings.Split(raw, ",")
	normalized := make([]string, 0, len(items))
	for _, item := range items {
		text := strings.TrimSpace(item)
		if text == "" {
			continue
		}
		normalized = append(normalized, text)
	}
	value := reflect.MakeSlice(sliceType, len(normalized), len(normalized))
	for idx, item := range normalized {
		value.Index(idx).SetString(item)
	}
	return value, nil
}

func envKey(prefix string, path []string) string {
	tokens := make([]string, 0, len(path)*2+1)
	if prefix != "" {
		tokens = append(tokens, prefix)
	}
	for _, item := range path {
		tokens = append(tokens, identifierTokens(item)...)
	}
	return strings.Join(tokens, "_")
}

func identifierTokens(name string) []string {
	if name == "" {
		return nil
	}

	tokens := make([]string, 0, 4)
	for len(name) > 0 {
		matched := ""
		for _, initialism := range knownInitialisms {
			if strings.HasPrefix(name, initialism) {
				matched = initialism
				break
			}
		}
		if matched != "" {
			tokens = append(tokens, matched)
			name = name[len(matched):]
			continue
		}

		runes := []rune(name)
		size := nextTokenSize(runes)
		tokens = append(tokens, strings.ToUpper(string(runes[:size])))
		name = string(runes[size:])
	}

	return tokens
}

func nextTokenSize(runes []rune) int {
	if len(runes) == 0 {
		return 0
	}

	if unicode.IsDigit(runes[0]) {
		size := 1
		for size < len(runes) && unicode.IsDigit(runes[size]) {
			size++
		}
		return size
	}

	if unicode.IsUpper(runes[0]) {
		if len(runes) > 1 && unicode.IsLower(runes[1]) {
			size := 2
			for size < len(runes) && (unicode.IsLower(runes[size]) || unicode.IsDigit(runes[size])) {
				size++
			}
			return size
		}

		size := 1
		for size < len(runes) && unicode.IsUpper(runes[size]) {
			if size+1 < len(runes) && unicode.IsLower(runes[size+1]) {
				break
			}
			size++
		}
		return size
	}

	size := 1
	for size < len(runes) && (unicode.IsLower(runes[size]) || unicode.IsDigit(runes[size])) {
		size++
	}
	return size
}

func clonePath(path []string) []string {
	if len(path) == 0 {
		return nil
	}
	return append([]string(nil), path...)
}
