# EasyStatistics

Buffered, SQL-backed counters for Go applications. Add values in memory, group them by fields and UTC time buckets, and let background workers upload the accumulated changes.

The library requires **Go 1.20 or later** and uses only the standard library. You supply a `*sql.DB` and a compatible database driver; the driver may require a newer Go version.

## Install

```sh
go get github.com/LazyEasyDev/EasyStatistics
```

## Quick Start

This example uses SQLite and a local database file. In a Go module, install its driver:

```sh
go get modernc.org/sqlite
```

```go
package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	EasyStatistics "github.com/LazyEasyDev/EasyStatistics"
	_ "modernc.org/sqlite"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	database, err := sql.Open("sqlite", "statistics.db")
	if err != nil {
		return err
	}
	defer database.Close()

	if err := EasyStatistics.Init(context.Background(), database, EasyStatistics.SQLSQLite, func(err error) {
		log.Printf("statistics: %v", err)
	}); err != nil {
		return err
	}
	defer EasyStatistics.Close()

	score, err := EasyStatistics.NewStatistics(
		"mining_score",
		5*time.Second,
		[]string{"user", "activity"},
		[]EasyStatistics.Dimension{
			{
				OrderFields:    []string{"user", "activity"},
				GroupIntervals: []EasyStatistics.GroupInterval{EasyStatistics.GroupMonth, EasyStatistics.GroupForever},
				ClearAfter:     90 * 24 * time.Hour,
			},
			{
				OrderFields:    []string{"user"},
				GroupIntervals: []EasyStatistics.GroupInterval{EasyStatistics.GroupForever},
			},
		},
	)
	if err != nil {
		return err
	}

	values := map[string]any{"user": "alice", "activity": "mining"}
	if err := score.Add(values, 10); err != nil {
		return err
	}
	if err := score.Add(values, -3); err != nil {
		return err
	}

	record, err := score.Get(
		[]EasyStatistics.OrderField{{Name: "user", Value: "alice"}},
		EasyStatistics.GroupForever,
		"forever",
	)
	if err != nil {
		return err
	}
	if record == nil {
		fmt.Println("No uploaded total yet.")
	} else {
		fmt.Println(record.Counter, record.LastUpdateTime)
	}
	return nil
}
```

Run the example with `go run .`. It adds a net score of 7 and attempts a final upload when it exits. **The query reads SQL, not the local buffer**, so a new database may have no result yet. With an existing database, the result may show the previously uploaded total.

In a server, initialize and register statistics **once during startup**, keep them alive while serving requests, and close them only during application shutdown. Keep the database pool open until statistics shutdown finishes.

## Common Usage

### Define Dimensions

A statistic declares all input field names. Each dimension selects an ordered subset of those fields and one or more time intervals.

The quick start updates three counters for each addition:

| Dimension | Interval | Meaning |
| --- | --- | --- |
| `user`, `activity` | `GroupMonth` | Monthly score for that user's activity |
| `user`, `activity` | `GroupForever` | All-time score for that user's activity |
| `user` | `GroupForever` | All-time score across that user's activities |

Values are not predeclared: new users and activities can arrive through `Add`. The update interval must be positive. An ordered field sequence may appear only once per statistic; put all its intervals in the same dimension.

### Add Counters

```go
if err := score.Add(map[string]any{"user": "alice", "activity": "mining"}, 10); err != nil {
	return err
}
```

- Supply exactly the declared fields, even when querying a dimension that uses only some of them. Input map order does not matter.
- Use strings, booleans, signed or unsigned integers, including `uintptr`, or named types with those underlying kinds. Floats, nil, pointers, slices, maps, structs, and complex values are rejected.
- Values normalize to strings: `int(7)`, `uint64(7)`, and `"7"` identify the same value. Likewise, `true` and `"true"` match. Strings must be valid UTF-8; empty strings are allowed.
- Positive, negative, and zero deltas are accepted. Deltas and totals are `int64`; **keep accumulated totals within its range**. Arithmetic is not checked for overflow and can wrap.
- `Add` performs no SQL work. It can be called concurrently, but callers must not modify an input map while it is being read.

### Find A Registered Statistic

```go
score, err := EasyStatistics.GetStatistics("mining_score")
if err != nil {
	return err
}
```

This returns the same instance created by `NewStatistics`. It does not create another worker or read SQL. An unknown name, an uninitialized library, or a canceled service returns an error.

### Query Counters

Choose exactly one configured dimension, preserve its field order, and use one of its configured intervals:

```go
fields := []EasyStatistics.OrderField{
	{Name: "user", Value: "alice"},
	{Name: "activity", Value: "mining"},
}
record, err := score.GetWithTime(fields, EasyStatistics.GroupMonth, time.Now())
if err != nil {
	return err
}
if record != nil {
	fmt.Println(record.Counter)
}
```

The three query methods differ only in how you select the bucket:

| Method | Bucket Input |
| --- | --- |
| `GetWithTime` | A `time.Time`, converted to UTC |
| `GetWithUnixTimeSecond` | Unix seconds, not milliseconds |
| `Get` | A canonical bucket string from the table below |

All queries read **uploaded SQL data only**. A missing row returns `(nil, nil)`; a stored zero counter returns a non-nil record. Invalid input or a database failure returns an error.

`Record` includes the statistic name, normalized field values, interval, bucket string, counter, and `LastUpdateTime`. That timestamp is the latest persisted local `Add` time, not the upload time.

### Choose Time Buckets

| Constant | Example For `Get` |
| --- | --- |
| `GroupSec` | `2026-10-07T12:34:56Z` |
| `GroupMinute` | `2026-10-07T12:34Z` |
| `GroupHour` | `2026-10-07T12Z` |
| `GroupDay` | `2026-10-07` |
| `GroupWeek` | `2026-W41` |
| `GroupMonth` | `2026-10` |
| `GroupYear` | `2026` |
| `GroupForever` | `forever` |

All buckets use UTC. Weeks start on Monday and follow ISO week years. `GroupForever` ignores timestamps; its string form accepts `forever` or `FOREVER`. `Add` uses the current local call time, with no caller-supplied event timestamp.

### Expire Inactive Records

Set `Dimension.ClearAfter` to delete records that have not received a **persisted** addition within that duration. Zero disables expiry; negative durations are invalid. Positive durations round up to whole seconds.

Retention applies to every value and interval in that dimension, including `GroupForever`. It is based on inactivity, not the age of the bucket. Cleanup runs before update attempts, including idle cycles and final uploads, using the database clock. Buffered data can later recreate an expired row. Keep application and database clocks synchronized, and use consistent policies across processes.

### Handle Errors

`Init` accepts a `func(error)` for worker failures; pass `nil` to disable notifications. The callback receives batch-creation, retention-cleanup, upload, and batch-marker-deletion failures, wrapped with the statistic name.

The callback runs synchronously, outside the buffer and service locks. Different workers may call it concurrently. Keep it quick and protect shared application state. **Do not call `Close` or `Wait` synchronously from the callback**, because they wait for that worker to exit.

Initialization, registration, lookup, `Add`, and query errors return directly to their callers. The library does not expose shared error sentinels or store a latest-error status.

### Shut Down

Call `EasyStatistics.Close()` to stop normal operation and wait for workers to attempt their remaining uploads. Alternatively, cancel the context passed to `Init`, then call `EasyStatistics.Wait()` to observe completion.

| Operation | Behavior |
| --- | --- |
| `Close` | Cancels the service and waits for its workers |
| `Wait` | Waits for cancellation or `Close`, then waits for workers |
| Repeated `Close` or `Wait` | Observes the completed shutdown; does not restart uploads |

Workers attempt pending and active batches in order, continuing only while successful. **A failed final attempt is reported once and that worker exits without retrying.** Remaining data stays in memory and is lost when the process exits.

`Close` and `Wait` return nil after initialized shutdown, even when an upload failed. Their return values are not delivery confirmation; use the callback for worker failures. Calls before initialization return an error. Registrations, lookups, additions, and queries reject a canceled service.

The library never closes or reconfigures your database pool. Close it after `Close` or `Wait` returns. Shutdown still waits for ongoing SQL attempts and callbacks to finish.

## Database Options

Pass the dialect matching your database to `Init` and import its driver in your application.

| Database | Dialect |
| --- | --- |
| PostgreSQL | `SQLPostgreSQL` |
| MySQL | `SQLMySQL` |
| MariaDB | `SQLMariaDB` |
| TiDB | `SQLTiDB` |
| SQLite | `SQLSQLite` |
| SQL Server | `SQLServer` |
| PostgreSQL-compatible GaussDB | `SQLGaussDB` |
| Oracle | `SQLOracle` |

`Init` checks connectivity, creates missing tables, and checks that required columns are readable. It does not migrate existing tables or fully validate their types and constraints. The database account needs table-creation permission for initial setup and read/write/delete permissions during operation.

SQL dialect support is implemented for the databases above; it is not a claim that every driver or server version has been live-tested. SQLite was exercised during development. Validate your chosen driver and deployment before production use.

## API Reference

```text
Init(ctx context.Context, database *sql.DB, dialect SQLDialect, onError func(error)) error
NewStatistics(name string, updateInterval time.Duration, fields []string, dimensions []Dimension) (*Statistics, error)
GetStatistics(name string) (*Statistics, error)
Close() error
Wait() error

(*Statistics).Add(values map[string]any, delta int64) error
(*Statistics).Get(orderFields []OrderField, interval GroupInterval, groupedTime string) (*Record, error)
(*Statistics).GetWithTime(orderFields []OrderField, interval GroupInterval, timestamp time.Time) (*Record, error)
(*Statistics).GetWithUnixTimeSecond(orderFields []OrderField, interval GroupInterval, timestamp int64) (*Record, error)
```

There is one successful initialization per process, even after shutdown. A failed initialization may be retried. Re-register definitions after a process restart. Statistic names must be unique in the process; names and field names must contain 1 to 255 UTF-8 bytes, with no surrounding whitespace or control characters. Configuration slices are copied at registration.

## Design Notes

### Buffering And Retries

Each statistic has its own active buffer, pending batch, update interval, and worker. At an update, the worker detaches the active map and replaces it with an empty one; new additions continue while SQL work runs outside the buffer lock. Input normalization and bucket/key creation also happen before `Add` takes that lock.

A failed background attempt waits **10 seconds** before retrying, including when the database pool is closed. The same pending batch is retained; no newer batch is submitted first. Successful attempts restart the statistic's normal update interval. SQL queries and upload attempts use **30-second contexts**, which require driver cancellation support to be effective.

Cleanup runs in the same worker before submission, not in a separate timer or worker. It also runs on idle update cycles. With no registered statistics, no periodic cleanup runs.

### Storage And Batch Identity

`easy_statistics` stores counters and their identities. Ordered field names and normalized values occupy separate, URL-escaped text columns. SHA-256 keys identify dimensions and records without relying on database text collations. SQL timestamps use Unix seconds; delayed uploads never lower a record's last-update timestamp.

`easy_statistics_batches` stores a random 256-bit batch ID and a database-generated creation timestamp. Counter changes and the marker commit in one transaction. Retrying an uncertain commit checks the same ID, so a retained marker prevents reapplying that batch. After confirmation, the marker is deleted; a failed deletion does not reapply confirmed counter changes.

Markers older than **30 days** are pruned during update attempts. This cleanup is independent of dimension retention. For large counter tables, an optional index on `(dimension_id, last_update_time)` can speed up expiry; the library does not add it automatically.

### Reliability Limits

This library is for **approximate statistics**, not a durable event ledger:

- A crash loses buffered additions because buffers exist only in memory.
- A failed final upload is not retried during shutdown.
- An unavailable database can increase memory use as new values accumulate.
- An uncertain batch resumed after its marker expires may be counted again.
- Counter arithmetic can overflow if callers allow totals outside the `int64` range.

SQL transactions protect shared counter updates across processes, but RAM buffering and finite marker retention are not an unlimited exactly-once guarantee.