package EasyStatistics

import "time"

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

type Record struct {
	Name           string
	OrderFields    []OrderField
	GroupInterval  GroupInterval
	GroupedTimeStr string
	LastUpdateTime time.Time
	Counter        int64
}

type OrderField struct {
	Name  string
	Value any
}
