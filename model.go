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
	GroupDay     GroupInterval = "DAY"
	GroupWeek    GroupInterval = "WEEK"
	GroupMonth   GroupInterval = "MONTH"
	GroupYear    GroupInterval = "YEAR"
	GroupForever GroupInterval = "FOREVER"
)

type Dimension struct {
	OrderFields    []string        `json:"order_fields"`
	GroupIntervals []GroupInterval `json:"group_intervals"`
	ClearAfter     time.Duration   `json:"clear_after"`
	Count          int64           `json:"count"`
	encodedFields  string
}

type StatisticsDefinition struct {
	Name           string        `json:"name"`
	Fields         []string      `json:"fields"`
	Dimensions     []Dimension   `json:"dimensions"`
	UpdateInterval time.Duration `json:"update_interval"`
}

type DimensionCount struct {
	Name        string   `json:"name"`
	OrderFields []string `json:"order_fields"`
	Count       int64    `json:"count"`
}

type OrderField struct {
	Name  string `json:"name"`
	Value any    `json:"value"`
}

type Record struct {
	Name           string        `json:"name"`
	OrderFields    []OrderField  `json:"order_fields"`
	GroupInterval  GroupInterval `json:"group_interval"`
	GroupedTimeStr string        `json:"grouped_time"`
	LastUpdateTime time.Time     `json:"last_update_time"`
	Counter        int64         `json:"counter"`
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

func recordID(name, dimension, value string, interval GroupInterval, bucket int64) string {
	identity, _ := json.Marshal([5]string{name, dimension, value, string(interval), strconv.FormatInt(bucket, 10)})
	digest := sha256.Sum256(identity)
	var encoded [sha256.Size * 2]byte
	hex.Encode(encoded[:], digest[:])
	return string(encoded[:])
}
