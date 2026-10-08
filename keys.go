package EasyStatistics

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"unicode/utf8"
)

func normalizeValue(value any) (string, error) {
	if value == nil {
		return "", fmt.Errorf("dimension values cannot be nil")
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.String:
		text := reflected.String()
		if !utf8.ValidString(text) {
			return "", fmt.Errorf("dimension strings must be valid UTF-8")
		}
		return text, nil
	case reflect.Bool:
		return strconv.FormatBool(reflected.Bool()), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(reflected.Int(), 10), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return strconv.FormatUint(reflected.Uint(), 10), nil
	default:
		return "", fmt.Errorf("unsupported dimension value %T: use strings, booleans, or integers", value)
	}
}

func encodeSequence(parts []string) string {
	escaped := make([]string, len(parts))
	for index, part := range parts {
		escaped[index] = url.QueryEscape(part)
	}
	return strings.Join(escaped, ":")
}

func encodeFields(fields []OrderField) (string, string, []OrderField, error) {
	names := make([]string, len(fields))
	values := make([]string, len(fields))
	normalized := make([]OrderField, len(fields))
	seen := make(map[string]bool, len(fields))
	for index, field := range fields {
		if field.Name == "" || !utf8.ValidString(field.Name) || seen[field.Name] {
			return "", "", nil, fmt.Errorf("invalid or repeated dimension field %q", field.Name)
		}
		seen[field.Name] = true
		text, err := normalizeValue(field.Value)
		if err != nil {
			return "", "", nil, fmt.Errorf("field %q: %w", field.Name, err)
		}
		names[index] = field.Name
		values[index] = text
		normalized[index] = OrderField{Name: field.Name, Value: text}
	}
	return encodeSequence(names), encodeSequence(values), normalized, nil
}

func dimensionID(name, fields string) string {
	identity, _ := json.Marshal([]string{name, fields})
	digest := sha256.Sum256(identity)
	return hex.EncodeToString(digest[:])
}

func recordID(name, dimension, value string, interval GroupInterval, bucket int64) string {
	identity, _ := json.Marshal([]string{name, dimension, value, string(interval), strconv.FormatInt(bucket, 10)})
	digest := sha256.Sum256(identity)
	return hex.EncodeToString(digest[:])
}
