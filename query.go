package EasyStatistics

import (
	"errors"
	"fmt"
	"time"
)

type Query struct {
	statistic *statistics
	fields    []OrderField
	interval  GroupInterval
	groupTime any
}

func (query *Query) Field(name string, value any) *Query {
	query.fields = append(query.fields, OrderField{Name: name, Value: value})
	return query
}

func (query *Query) GroupInterval(interval GroupInterval) *Query {
	query.interval = interval
	return query
}

func (query *Query) GroupTime(value any) *Query {
	query.groupTime = value
	return query
}

func (query *Query) Get() (*Record, error) {
	if query == nil || query.statistic == nil {
		return nil, errors.New("query requires a statistics instance")
	}
	if query.interval == "" {
		return nil, errors.New("query group interval is required")
	}
	var bucket int64
	var label string
	var err error
	switch value := query.groupTime.(type) {
	case nil:
		if query.interval != GroupForever {
			return nil, errors.New("query group time is required")
		}
		bucket, label, err = bucketFor(query.interval, time.Time{})
	case string:
		bucket, label, err = parseBucket(query.interval, value)
	case time.Time:
		bucket, label, err = bucketFor(query.interval, value)
	case int64:
		bucket, label, err = bucketFor(query.interval, time.Unix(value, 0))
	default:
		return nil, fmt.Errorf("unsupported group time %T: use a grouped string, time.Time, or int64 Unix seconds", query.groupTime)
	}
	if err != nil {
		return nil, err
	}
	return query.statistic.get(query.fields, query.interval, bucket, label)
}
