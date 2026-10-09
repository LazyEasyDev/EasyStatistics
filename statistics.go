package EasyStatistics

import (
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"go/token"
	"hash/fnv"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"
)

const shardNum = 8

type Statistics struct {
	name           string
	fields         []string
	dimensions     []Dimension
	rowsPerAdd     int
	updateInterval time.Duration
	service        *service
	shards         [shardNum]counterShard
	pending        *uploadBatch
}

type counterShard struct {
	mu     sync.Mutex
	active map[counterKey]counterRow
	_      [112]byte
}

type counterKey struct {
	dimension string
	value     string
	interval  GroupInterval
	bucket    int64
}

type counterChange struct {
	key        counterKey
	row        counterRow
	shardIndex int
}

func (key counterKey) shardIndex() int {
	hasher := fnv.New64a()
	for _, text := range [3]string{key.dimension, key.value, string(key.interval)} {
		hasher.Write([]byte(text))
		hasher.Write([]byte{0})
	}
	var bucket [8]byte
	binary.LittleEndian.PutUint64(bucket[:], uint64(key.bucket))
	hasher.Write(bucket[:])
	return int(hasher.Sum64() % uint64(shardNum))
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
		if len(field) > 255 || field == "_" || !token.IsIdentifier(field) {
			return nil, nil, fmt.Errorf("statistics field %q must be a non-blank Go identifier of at most 255 bytes", field)
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
		service: initialized,
	}
	for index := range statistic.shards {
		statistic.shards[index].active = make(map[counterKey]counterRow)
	}
	initialized.mu.Lock()
	if initialized.isClosing() {
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
	if initialized.isClosing() {
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
	var localChanges [8]counterChange
	changes := localChanges[:0]
	if statistic.rowsPerAdd > len(localChanges) {
		changes = make([]counterChange, 0, statistic.rowsPerAdd)
	}
	var selectedShards [shardNum]bool
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
			key := counterKey{dimension: dimension.encodedFields, value: encodedValues, interval: interval, bucket: bucket}
			shardIndex := key.shardIndex()
			selectedShards[shardIndex] = true
			changes = append(changes, counterChange{
				key: key, shardIndex: shardIndex,
				row: counterRow{
					name:        statistic.name,
					dimensionID: dimension.id, dimension: dimension.encodedFields, value: encodedValues,
					interval: interval, bucket: bucket, updated: timestamp.Unix(), counter: delta,
					clearAfter: dimension.ClearAfter,
				},
			})
		}
	}
	for index, selected := range selectedShards {
		if selected {
			statistic.shards[index].mu.Lock()
		}
	}
	defer func() {
		for index := len(selectedShards) - 1; index >= 0; index-- {
			if selectedShards[index] {
				statistic.shards[index].mu.Unlock()
			}
		}
	}()
	if initialized.isClosing() {
		return errors.New("EasyStatistics is closed")
	}
	for _, changed := range changes {
		shard := &statistic.shards[changed.shardIndex]
		previous, exists := shard.active[changed.key]
		row := changed.row
		row.counter += previous.counter
		if exists && previous.updated > row.updated {
			row.updated = previous.updated
		}
		shard.active[changed.key] = row
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
	if initialized.isClosing() {
		return nil, errors.New("EasyStatistics is closed")
	}
	var matched *Dimension
	for index := range statistic.dimensions {
		dimension := &statistic.dimensions[index]
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
					matched = dimension
					break
				}
			}
		}
		if matched != nil {
			break
		}
	}
	if matched == nil {
		return nil, errors.New("query fields and interval must match a configured dimension in order")
	}
	encodedValues, normalized, err := encodeValues(orderFields)
	if err != nil {
		return nil, err
	}
	identity := recordID(statistic.name, matched.encodedFields, encodedValues, interval, bucket)
	row, err := initialized.backend.getRow(identity)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("query statistics: %w", err)
	}
	if row.name != statistic.name || row.dimensionID != matched.id || row.dimension != matched.encodedFields || row.value != encodedValues || row.interval != interval || row.bucket != bucket {
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
		GroupDay:   "2006-01-02",
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

func (statistic *Statistics) nextBatch() *uploadBatch {
	if statistic.pending != nil {
		return statistic.pending
	}
	hasRows := false
	for index := range statistic.shards {
		shard := &statistic.shards[index]
		shard.mu.Lock()
		hasRows = len(shard.active) != 0
		shard.mu.Unlock()
		if hasRows {
			break
		}
	}
	if !hasRows {
		return nil
	}

	var detached [shardNum]map[counterKey]counterRow
	rowCount := 0
	for index := range statistic.shards {
		shard := &statistic.shards[index]
		active := make(map[counterKey]counterRow)
		shard.mu.Lock()
		detached[index] = shard.active
		shard.active = active
		shard.mu.Unlock()
		rowCount += len(detached[index])
	}
	batch := &uploadBatch{rows: make(map[string]counterRow, rowCount)}
	for _, rows := range detached {
		for _, row := range rows {
			row.id = recordID(row.name, row.dimension, row.value, row.interval, row.bucket)
			batch.rows[row.id] = row
		}
	}
	statistic.pending = batch
	return batch
}

func (statistic *Statistics) reportError(err error) {
	if err != nil && statistic.service.onError != nil {
		statistic.service.onError(fmt.Errorf("statistics %q: %w", statistic.name, err))
	}
}

func (statistic *Statistics) upload(batch *uploadBatch) error {
	if err := statistic.service.backend.submit(statistic.service.ctx, batch, statistic.reportError); err != nil {
		return err
	}
	statistic.pending = nil
	return nil
}

func (statistic *Statistics) run() {
	defer statistic.service.workers.Done()
	timer := time.NewTimer(statistic.updateInterval)
	defer timer.Stop()
	for {
		select {
		case <-statistic.service.closing:
		case <-timer.C:
		}
		closing := statistic.service.isClosing()
		batch := statistic.nextBatch()
		if batch == nil && closing {
			return
		}
		if err := statistic.upload(batch); err != nil {
			statistic.service.stop()
			return
		}
		timer.Reset(statistic.updateInterval)
	}
}
