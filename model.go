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
	"time"
	"unicode/utf8"
)

type GroupInterval string

const (
	GroupSec     GroupInterval = "SECOND"
	GroupMinute  GroupInterval = "MINUTE"
	GroupHour    GroupInterval = "HOUR"
	GroupDay     GroupInterval = "DAY"
	GroupWeek    GroupInterval = "WEEK"
	GroupMonth   GroupInterval = "MONTH"
	GroupYear    GroupInterval = "YEAR"
	GroupForever GroupInterval = "FOREVER"
)

type Dimension struct {
	OrderFields    []string
	GroupIntervals []GroupInterval
	ClearAfter     time.Duration
	encodedFields  string
	id             string
}

type OrderField struct {
	Name  string
	Value any
}

type Record struct {
	Name           string
	OrderFields    []OrderField
	GroupInterval  GroupInterval
	GroupedTimeStr string
	LastUpdateTime time.Time
	Counter        int64
}

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

func encodeValues(fields []OrderField) (string, []OrderField, error) {
	values := make([]string, len(fields))
	normalized := make([]OrderField, len(fields))
	for index, field := range fields {
		text, err := normalizeValue(field.Value)
		if err != nil {
			return "", nil, fmt.Errorf("field %q: %w", field.Name, err)
		}
		values[index] = text
		normalized[index] = OrderField{Name: field.Name, Value: text}
	}
	return encodeSequence(values), normalized, nil
}

func dimensionID(name, fields string) string {
	identity, _ := json.Marshal([]string{name, fields})
	digest := sha256.Sum256(identity)
	return hex.EncodeToString(digest[:])
}

func recordID(name, dimension, value string, interval GroupInterval, bucket int64) string {
	identity, _ := json.Marshal([5]string{name, dimension, value, string(interval), strconv.FormatInt(bucket, 10)})
	digest := sha256.Sum256(identity)
	var encoded [sha256.Size * 2]byte
	hex.Encode(encoded[:], digest[:])
	return string(encoded[:])
}
