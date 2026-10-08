package EasyStatistics

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"
)

const (
	retryDelay = 10 * time.Second
	sqlTimeout = 30 * time.Second
)

type Statistics struct {
	name           string
	fields         []string
	dimensions     []Dimension
	rowsPerAdd     int
	updateInterval time.Duration
	service        *service
	onError        func(error)
	mu             sync.Mutex
	active         map[string]counterRow
	pending        *uploadBatch
}

func validateIdentifier(name string) error {
	if len(name) == 0 || len(name) > 255 || !utf8.ValidString(name) || strings.TrimSpace(name) != name {
		return fmt.Errorf("identifier %q must contain 1 to 255 UTF-8 bytes without surrounding whitespace", name)
	}
	for _, character := range name {
		if unicode.IsControl(character) {
			return fmt.Errorf("identifier %q contains a control character", name)
		}
	}
	return nil
}

func validateDefinition(name string, updateInterval time.Duration, fields []string, dimensions []Dimension) ([]string, []Dimension, error) {
	if err := validateIdentifier(name); err != nil {
		return nil, nil, err
	}
	if updateInterval <= 0 || len(fields) == 0 || len(dimensions) == 0 {
		return nil, nil, errors.New("positive update interval, fields, and dimensions are required")
	}
	known := make(map[string]bool, len(fields))
	for _, field := range fields {
		if err := validateIdentifier(field); err != nil {
			return nil, nil, err
		}
		if known[field] {
			return nil, nil, fmt.Errorf("repeated statistics field %q", field)
		}
		known[field] = true
	}
	seenDimensions := make(map[string]bool, len(dimensions))
	copied := make([]Dimension, len(dimensions))
	for index, dimension := range dimensions {
		if len(dimension.OrderFields) == 0 || len(dimension.GroupIntervals) == 0 {
			return nil, nil, errors.New("dimension fields and group intervals are required")
		}
		if dimension.ClearAfter < 0 {
			return nil, nil, errors.New("dimension ClearAfter cannot be negative")
		}
		seenFields := make(map[string]bool, len(dimension.OrderFields))
		for _, field := range dimension.OrderFields {
			if !known[field] || seenFields[field] {
				return nil, nil, fmt.Errorf("unknown or repeated dimension field %q", field)
			}
			seenFields[field] = true
		}
		identity := encodeSequence(dimension.OrderFields)
		if seenDimensions[identity] {
			return nil, nil, fmt.Errorf("repeated dimension %q", identity)
		}
		seenDimensions[identity] = true
		seenIntervals := make(map[GroupInterval]bool, len(dimension.GroupIntervals))
		for _, interval := range dimension.GroupIntervals {
			if _, _, err := bucketFor(interval, time.Unix(0, 0)); err != nil {
				return nil, nil, err
			}
			if seenIntervals[interval] {
				return nil, nil, fmt.Errorf("repeated group interval %q", interval)
			}
			seenIntervals[interval] = true
		}
		copied[index] = Dimension{
			OrderFields:    append([]string(nil), dimension.OrderFields...),
			GroupIntervals: append([]GroupInterval(nil), dimension.GroupIntervals...),
			ClearAfter:     dimension.ClearAfter,
			encodedFields:  identity,
			id:             dimensionID(name, identity),
		}
	}
	return append([]string(nil), fields...), copied, nil
}

func (initialized *service) newStatistics(name string, updateInterval time.Duration, fields []string, dimensions []Dimension) (*Statistics, error) {
	copiedFields, copiedDimensions, err := validateDefinition(name, updateInterval, fields, dimensions)
	if err != nil {
		return nil, err
	}
	rowsPerAdd := 0
	for _, dimension := range copiedDimensions {
		rowsPerAdd += len(dimension.GroupIntervals)
	}
	statistic := &Statistics{
		name: name, fields: copiedFields, dimensions: copiedDimensions,
		rowsPerAdd: rowsPerAdd, updateInterval: updateInterval,
		service: initialized, onError: initialized.onError, active: make(map[string]counterRow),
	}
	initialized.mu.Lock()
	if initialized.ctx.Err() != nil {
		initialized.mu.Unlock()
		return nil, errors.New("EasyStatistics is closed")
	}
	if _, exists := initialized.statistics[name]; exists {
		initialized.mu.Unlock()
		return nil, fmt.Errorf("statistics %q already exists", name)
	}
	initialized.statistics[name] = statistic
	initialized.workers.Add(1)
	initialized.mu.Unlock()
	go statistic.run()
	return statistic, nil
}

func (statistic *Statistics) Add(values map[string]any, delta int64) error {
	initialized := statistic.service
	if initialized.ctx.Err() != nil {
		return errors.New("EasyStatistics is closed")
	}
	if len(values) != len(statistic.fields) {
		return errors.New("Add requires exactly the declared statistics fields")
	}
	encoded := make(map[string]string, len(values))
	for _, field := range statistic.fields {
		value, exists := values[field]
		if !exists {
			return fmt.Errorf("missing statistics field %q", field)
		}
		text, err := normalizeValue(value)
		if err != nil {
			return fmt.Errorf("field %q: %w", field, err)
		}
		encoded[field] = url.QueryEscape(text)
	}
	timestamp := time.Now().UTC()
	changes := make([]counterRow, 0, statistic.rowsPerAdd)
	for _, dimension := range statistic.dimensions {
		ordered := make([]string, len(dimension.OrderFields))
		for index, field := range dimension.OrderFields {
			ordered[index] = encoded[field]
		}
		encodedValues := strings.Join(ordered, ":")
		for _, interval := range dimension.GroupIntervals {
			bucket, _, err := bucketFor(interval, timestamp)
			if err != nil {
				return err
			}
			changes = append(changes, counterRow{
				id: recordID(statistic.name, dimension.encodedFields, encodedValues, interval, bucket), name: statistic.name,
				dimensionID: dimension.id, dimension: dimension.encodedFields, value: encodedValues,
				interval: interval, bucket: bucket, updated: timestamp.Unix(), counter: delta,
			})
		}
	}
	statistic.mu.Lock()
	defer statistic.mu.Unlock()
	if initialized.ctx.Err() != nil {
		return errors.New("EasyStatistics is closed")
	}
	for _, changed := range changes {
		previous, exists := statistic.active[changed.id]
		changed.counter += previous.counter
		if exists && previous.updated > changed.updated {
			changed.updated = previous.updated
		}
		statistic.active[changed.id] = changed
	}
	return nil
}

func (statistic *Statistics) GetWithTime(orderFields []OrderField, interval GroupInterval, timestamp time.Time) (*Record, error) {
	bucket, label, err := bucketFor(interval, timestamp)
	if err != nil {
		return nil, err
	}
	return statistic.get(orderFields, interval, bucket, label)
}

func (statistic *Statistics) GetWithUnixTimeSecond(orderFields []OrderField, interval GroupInterval, timestamp int64) (*Record, error) {
	return statistic.GetWithTime(orderFields, interval, time.Unix(timestamp, 0))
}

func (statistic *Statistics) Get(orderFields []OrderField, interval GroupInterval, groupedTime string) (*Record, error) {
	bucket, label, err := parseBucket(interval, groupedTime)
	if err != nil {
		return nil, err
	}
	return statistic.get(orderFields, interval, bucket, label)
}

func (statistic *Statistics) get(orderFields []OrderField, interval GroupInterval, bucket int64, label string) (*Record, error) {
	initialized := statistic.service
	if initialized.ctx.Err() != nil {
		return nil, errors.New("EasyStatistics is closed")
	}
	matched := false
	for _, dimension := range statistic.dimensions {
		if len(dimension.OrderFields) != len(orderFields) {
			continue
		}
		matches := true
		for index, field := range dimension.OrderFields {
			if orderFields[index].Name != field {
				matches = false
				break
			}
		}
		if matches {
			for _, configured := range dimension.GroupIntervals {
				if interval == configured {
					matched = true
				}
			}
		}
	}
	if !matched {
		return nil, errors.New("query fields and interval must match a configured dimension in order")
	}
	encodedFields, encodedValues, normalized, err := encodeFields(orderFields)
	if err != nil {
		return nil, err
	}
	identity := recordID(statistic.name, encodedFields, encodedValues, interval, bucket)
	ctx, cancel := context.WithTimeout(initialized.ctx, sqlTimeout)
	defer cancel()
	row, err := initialized.backend.readRow(ctx, initialized.backend.db, identity, false)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("query statistics: %w", err)
	}
	if row.name != statistic.name || row.dimensionID != dimensionID(statistic.name, encodedFields) || row.dimension != encodedFields || row.value != encodedValues || row.interval != interval || row.bucket != bucket {
		return nil, errors.New("stored record identity does not match its key")
	}
	return &Record{
		Name: row.name, OrderFields: normalized, GroupInterval: interval, GroupedTimeStr: label,
		LastUpdateTime: time.Unix(row.updated, 0).UTC(), Counter: row.counter,
	}, nil
}

func bucketFor(interval GroupInterval, timestamp time.Time) (int64, string, error) {
	timestamp = timestamp.UTC()
	year, month, day := timestamp.Date()
	var start time.Time
	var label string
	switch interval {
	case GroupSec:
		start = time.Unix(timestamp.Unix(), 0).UTC()
		label = start.Format("2006-01-02T15:04:05Z")
	case GroupMinute:
		start = time.Date(year, month, day, timestamp.Hour(), timestamp.Minute(), 0, 0, time.UTC)
		label = start.Format("2006-01-02T15:04Z")
	case GroupHour:
		start = time.Date(year, month, day, timestamp.Hour(), 0, 0, 0, time.UTC)
		label = start.Format("2006-01-02T15Z")
	case GroupDay:
		start = time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
		label = start.Format("2006-01-02")
	case GroupWeek:
		start = time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
		start = start.AddDate(0, 0, -(int(start.Weekday())+6)%7)
		isoYear, week := start.ISOWeek()
		label = fmt.Sprintf("%04d-W%02d", isoYear, week)
	case GroupMonth:
		start = time.Date(year, month, 1, 0, 0, 0, 0, time.UTC)
		label = start.Format("2006-01")
	case GroupYear:
		start = time.Date(year, time.January, 1, 0, 0, 0, 0, time.UTC)
		label = start.Format("2006")
	case GroupForever:
		return 0, "forever", nil
	default:
		return 0, "", fmt.Errorf("unsupported group interval %q", interval)
	}
	return start.Unix(), label, nil
}

func parseBucket(interval GroupInterval, label string) (int64, string, error) {
	if interval == GroupForever {
		if label != "forever" && label != "FOREVER" {
			return 0, "", fmt.Errorf("FOREVER requires grouped time \"forever\"")
		}
		return 0, "forever", nil
	}
	if interval == GroupWeek {
		if len(label) != 8 || label[4:6] != "-W" {
			return 0, "", fmt.Errorf("invalid ISO week %q: expected YYYY-Www", label)
		}
		january, err := time.Parse("2006-01-02", label[:4]+"-01-04")
		if err != nil {
			return 0, "", err
		}
		week, err := strconv.Atoi(label[6:])
		if err != nil || week < 1 || week > 53 {
			return 0, "", fmt.Errorf("invalid ISO week %q", label)
		}
		firstWeek, _, _ := bucketFor(GroupWeek, january)
		candidate := time.Unix(firstWeek, 0).UTC().AddDate(0, 0, (week-1)*7)
		bucket, canonical, err := bucketFor(interval, candidate)
		if canonical != label {
			return 0, "", fmt.Errorf("invalid ISO week %q", label)
		}
		return bucket, canonical, err
	}
	layouts := map[GroupInterval]string{
		GroupSec: "2006-01-02T15:04:05Z", GroupMinute: "2006-01-02T15:04Z",
		GroupHour: "2006-01-02T15Z", GroupDay: "2006-01-02",
		GroupMonth: "2006-01", GroupYear: "2006",
	}
	layout, exists := layouts[interval]
	if !exists {
		return 0, "", fmt.Errorf("unsupported group interval %q", interval)
	}
	timestamp, err := time.Parse(layout, label)
	if err != nil {
		return 0, "", fmt.Errorf("invalid grouped time %q: %w", label, err)
	}
	bucket, canonical, err := bucketFor(interval, timestamp)
	if label != canonical {
		return 0, "", fmt.Errorf("non-canonical grouped time %q: expected %q", label, canonical)
	}
	return bucket, canonical, err
}

func (statistic *Statistics) nextBatch() (*uploadBatch, error) {
	statistic.mu.Lock()
	if statistic.pending != nil || len(statistic.active) == 0 {
		batch := statistic.pending
		statistic.mu.Unlock()
		return batch, nil
	}
	statistic.mu.Unlock()

	var random [32]byte
	if _, err := rand.Read(random[:]); err != nil {
		return nil, fmt.Errorf("create batch identity: %w", err)
	}
	batch := &uploadBatch{id: hex.EncodeToString(random[:])}
	active := make(map[string]counterRow)

	statistic.mu.Lock()
	defer statistic.mu.Unlock()
	if statistic.pending != nil || len(statistic.active) == 0 {
		return statistic.pending, nil
	}
	batch.rows = statistic.active
	statistic.active = active
	statistic.pending = batch
	return batch, nil
}

func (statistic *Statistics) reportError(err error) {
	if err != nil && statistic.onError != nil {
		statistic.onError(fmt.Errorf("statistics %q: %w", statistic.name, err))
	}
}

func (statistic *Statistics) upload(parent context.Context, batch *uploadBatch) error {
	ctx, cancel := context.WithTimeout(parent, sqlTimeout)
	defer cancel()
	err := statistic.service.backend.cleanupBatches(ctx)
	if err == nil {
		for _, dimension := range statistic.dimensions {
			if err = statistic.service.backend.cleanupDimension(ctx, statistic.name, dimension); err != nil {
				break
			}
		}
	}
	if err == nil && batch != nil {
		err = statistic.service.backend.submit(ctx, batch)
	}
	if err != nil {
		statistic.reportError(err)
		return err
	}
	if batch != nil {
		statistic.mu.Lock()
		if statistic.pending == batch {
			statistic.pending = nil
		}
		statistic.mu.Unlock()
	}
	return nil
}

func (statistic *Statistics) run() {
	defer statistic.service.workers.Done()
	timer := time.NewTimer(statistic.updateInterval)
	defer timer.Stop()
	for {
		select {
		case <-statistic.service.ctx.Done():
			for {
				batch, err := statistic.nextBatch()
				if err != nil {
					statistic.reportError(err)
					return
				}
				if batch == nil {
					return
				}
				if err := statistic.upload(context.Background(), batch); err != nil {
					return
				}
			}
		case <-timer.C:
			batch, err := statistic.nextBatch()
			if err != nil {
				statistic.reportError(err)
			} else {
				err = statistic.upload(statistic.service.ctx, batch)
			}
			if err != nil {
				timer.Reset(retryDelay)
			} else {
				timer.Reset(statistic.updateInterval)
			}
		}
	}
}
