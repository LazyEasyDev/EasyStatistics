package EasyStatistics

import (
	"context"
	"errors"
	"fmt"
	"time"
)

const MaxQueryBuckets = 500

var ErrInvalidQuery = errors.New("invalid statistics query")

type Query struct {
	statistic    *statistics
	fields       []OrderField
	interval     GroupInterval
	groupTime    any
	groupTimeEnd any
	rangeSet     bool
	ctx          context.Context
}

type queryBucket struct {
	time  int64
	label string
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
	query.groupTimeEnd = nil
	query.rangeSet = false
	return query
}

func (query *Query) GroupTimeRange(start, end any) *Query {
	query.groupTime = start
	query.groupTimeEnd = end
	query.rangeSet = true
	return query
}

func (query *Query) WithContext(ctx context.Context) *Query {
	query.ctx = ctx
	return query
}

func (query *Query) Get() ([]Record, error) {
	if query == nil || query.statistic == nil {
		return nil, fmt.Errorf("%w: query requires a statistics instance", ErrInvalidQuery)
	}
	if query.ctx == nil {
		return nil, fmt.Errorf("%w: query context is required", ErrInvalidQuery)
	}
	if err := query.ctx.Err(); err != nil {
		return nil, err
	}
	buckets, err := query.buckets()
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidQuery, err)
	}
	return query.statistic.get(query.ctx, query.fields, query.interval, buckets)
}

func (query *Query) buckets() ([]queryBucket, error) {
	if query.interval == "" {
		return nil, errors.New("query group interval is required")
	}
	if query.rangeSet && query.interval == GroupForever {
		return nil, errors.New("FOREVER does not support a time range")
	}
	start, err := queryTimestamp(query.interval, query.groupTime)
	if err != nil {
		return nil, err
	}
	first, label, err := bucketFor(query.interval, start)
	if err != nil {
		return nil, err
	}
	if !query.rangeSet {
		return []queryBucket{{time: first, label: label}}, nil
	}
	if query.groupTime == nil || query.groupTimeEnd == nil {
		return nil, errors.New("query range requires start and end times")
	}
	end, err := queryTimestamp(query.interval, query.groupTimeEnd)
	if err != nil {
		return nil, err
	}
	if end.Before(start) {
		return nil, errors.New("query end time must not be before start time")
	}
	last, _, err := bucketFor(query.interval, end)
	if err != nil {
		return nil, err
	}
	buckets := make([]queryBucket, 0)
	for current := time.Unix(first, 0).UTC(); current.Unix() <= last; {
		if len(buckets) == MaxQueryBuckets {
			return nil, fmt.Errorf("query range must not exceed %d buckets", MaxQueryBuckets)
		}
		bucket, label, err := bucketFor(query.interval, current)
		if err != nil {
			return nil, err
		}
		buckets = append(buckets, queryBucket{time: bucket, label: label})
		switch query.interval {
		case GroupDay:
			current = current.AddDate(0, 0, 1)
		case GroupWeek:
			current = current.AddDate(0, 0, 7)
		case GroupMonth:
			current = current.AddDate(0, 1, 0)
		case GroupYear:
			current = current.AddDate(1, 0, 0)
		}
	}
	return buckets, nil
}

func queryTimestamp(interval GroupInterval, value any) (time.Time, error) {
	switch value := value.(type) {
	case nil:
		if interval != GroupForever {
			return time.Time{}, errors.New("query group time is required")
		}
		return time.Time{}, nil
	case string:
		bucket, _, err := parseBucket(interval, value)
		return time.Unix(bucket, 0).UTC(), err
	case time.Time:
		return value.UTC(), nil
	case int64:
		return time.Unix(value, 0).UTC(), nil
	default:
		return time.Time{}, fmt.Errorf("unsupported group time %T: use a grouped string, time.Time, or int64 Unix seconds", value)
	}
}
