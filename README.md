# EasyStatistics

Buffered, SQL-backed counters for Go applications. Add values in memory, group them by fields and UTC time buckets, and let background workers upload the accumulated changes.

EasyStatistics is intended for **coarse-grained SQL statistics**, with **day, week, month, year, and forever** grouping. Second, minute, and hour buckets are intentionally unsupported to limit the number of persisted time buckets and keep SQL table growth manageable. High-cardinality dimensions and long retention can still produce large tables.

For detailed second-, minute-, or hour-level statistics, choose a dedicated time-series or analytics project, such as a suitable NoSQL-based solution.

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

> [!WARNING]
> Only explicitly tagged fields participate in statistics. Fields without a `stat` tag, or with `stat:"-"`, are ignored. There is no fallback to the Go field name. A forgotten or misspelled tag can silently exclude a field; dimensions referencing an excluded field cause registration to fail.

```go
package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	EasyStatistics "github.com/LazyEasyDev/EasyStatistics"
	_ "modernc.org/sqlite"
)

type MiningValues struct {
	User     string `stat:"user"`
	Activity string `stat:"activity"`
}

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

	if err := EasyStatistics.Init(database, EasyStatistics.SQLSQLite); err != nil {
		return err
	}
	defer EasyStatistics.Close()

	score, err := EasyStatistics.NewStatistics[MiningValues](
		"mining_score",
		time.Minute,
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

	values := MiningValues{User: "alice", Activity: "mining"}
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

A statistic derives its input fields from participating `stat` tags on the struct type passed to `NewStatistics[T]`; there is no separate field-name list. `Statistics[T].Add` accepts that same type, so incompatible values and misspelled struct-literal fields are compiler errors. Each dimension selects an ordered subset of the tagged fields and one or more time intervals.

**Statistic names, not values types, must be unique within the current service.** The same `T` can be registered under different names, even with identical dimensions. Each named statistic has its own buffer, worker, update interval, and retention configuration; persisted keys include the statistic name, so counters remain separate. Calling `NewStatistics[T]` with an existing name returns an error instead of changing that statistic. Use `GetStatistics[T](name)` to retrieve its existing handle.

> [!WARNING]
> Untagged fields and fields with exactly `stat:"-"` are skipped both at registration and in `Add`, even if they have unsupported types. Nothing is inferred from the Go field name. Use explicit tags for every field you intend to collect, and check that dimensions reference their exact tag text.

`T` must be a non-pointer struct with at least one participating field. Participating fields must be exported, non-embedded strings, booleans, signed or unsigned integers, including `uintptr`, or named types with those underlying kinds. Registration rejects participating fields with unsupported types, including floats, interfaces, pointers, slices, maps, and nested structs. Ignored fields may have any type; ignored embedded structs are not traversed, even if their inner fields have tags. Struct shape and participating field types are validated once at registration; they are not expressible as a Go generic constraint.

| Struct Field Tag | Behavior |
| --- | --- |
| No `stat` tag | Ignore the field |
| `stat:"-"` | Ignore the field |
| `stat:"user"` | Collect the field with the name `user` |
| `stat:""` | Registration error, not a skip |

Participating tag names must contain **1 to 255 ASCII characters**, start with an uppercase or lowercase letter, and contain only letters, digits, or underscores thereafter:

```text
^[A-Za-z][A-Za-z0-9_]*$
```

Names are **case-sensitive**: `Xx` and `xx` are valid, distinct names; `1X` is invalid because it starts with a digit. Leading or trailing spaces, whitespace anywhere, Unicode, colons, asterisks, and other punctuation are rejected. Options such as `stat:"user,omitempty"` are unsupported. Names are never trimmed or case-converted. These restrictions apply to tag names only, not field values or statistic names.

Participating tag names must not contain duplicates. Each `Dimension.OrderFields` must be a nonempty ordered subset of these names, without duplicates. **Every item must match a participating tag exactly, including case.** Referencing an ignored field, the Go field name instead of its tag, or a different capitalization causes `NewStatistics[T]` to return an error. A field may be reused in different dimensions.

Struct declaration order and keyed struct-literal order do not determine stored keys; `Dimension.OrderFields` does. For example, `{"user", "activity"}` and `{"activity", "user"}` identify different dimensions. Query headers (`OrderField.Name`) must match a configured dimension exactly, including case and order. Dimension and query names remain runtime-validated strings. When retaining existing counters or renaming a Go field, keep its tag name and dimension order unchanged: changing them changes persisted dimension and record keys.

The quick start updates three counters for each addition:

| Dimension | Interval | Meaning |
| --- | --- | --- |
| `user`, `activity` | `GroupMonth` | Monthly score for that user's activity |
| `user`, `activity` | `GroupForever` | All-time score for that user's activity |
| `user` | `GroupForever` | All-time score across that user's activities |

Values are not predeclared: new users and activities can arrive through `Add`. An ordered field sequence may appear only once per statistic; put all its intervals in the same dimension.

### Choose An Upload Interval

`updateInterval` controls the normal wait before an upload and after each completed upload. Use **60 seconds to 1 hour** as the recommended range:

| Limit | Behavior |
| --- | --- |
| Minimum: **60 seconds** (`time.Minute`) | Enforced by `NewStatistics[T]`; smaller values return an error |
| Recommended maximum: **1 hour** (`time.Hour`) | Guidance only; larger values are accepted |

On elastic deployments, many instances may start, stop, or scale frequently. The 60-second minimum lets routine additions accumulate instead of issuing frequent SQL transactions, reducing database burden. `Close` still starts final draining immediately without waiting for `updateInterval`, so frequent instance restarts can still generate extra writes.

Keep the interval at or below one hour to limit how much data remains only in memory: longer intervals increase potential data loss if an instance crashes or is terminated before graceful draining finishes. This is a recommendation, not a durability guarantee or a hard one-hour loss window; SQL delays and retries can leave data buffered longer. Stop producers and allow `Close` to finish during planned shutdowns.

### Add Counters

```go
if err := score.Add(MiningValues{User: "alice", Activity: "mining"}, 10); err != nil {
	return err
}
```

- Pass a value of the registered struct type. Go permits omitted struct-literal fields: they take their zero values, rather than producing a missing-field error. `MiningValues{}` therefore supplies two valid empty strings.
- Field types are fixed by the struct. Registration validates supported kinds for participating fields, and the compiler checks assignments to them.
- Values normalize to strings: a numeric field holding `7` matches a query using `"7"`; `true` similarly matches `"true"`. Strings must be valid UTF-8. Empty strings, false, and zero integers are valid values. Value strings are not trimmed, so `""` and `" "` identify different values.
- Positive, negative, and zero deltas are accepted. Deltas and totals are `int64`; **keep accumulated totals within its range**. Arithmetic is not checked for overflow and can wrap.
- `Add` performs no SQL work and can be called concurrently. It reads only participating fields from the supplied struct value.

**If `Add` observes a closing or closed service, it returns nil and ignores the addition.** This also applies to old statistics handles after reinitialization; they do not send updates to the new service. The check before modifying the buffer also ignores a call that observes shutdown after preparing its changes.

This is intentional for high-frequency, best-effort statistics: request goroutines may still report counters during shutdown, and returning a closed-service error for every call could flood application logs. Unsupported struct schemas return registration errors; invalid UTF-8 values return errors from `Add` while the service is running. **A nil return is not a persistence guarantee:** ignored additions are neither buffered nor uploaded. Stop producers before calling `Close` when their final additions must be accepted.

### Find A Registered Statistic

```go
score, err := EasyStatistics.GetStatistics[MiningValues]("mining_score")
if err != nil {
	return err
}
```

This returns the same typed instance created by `NewStatistics`. Use the same values type; a different named struct type is rejected even if it has the same fields. It does not create another worker or read SQL. An unknown name, a mismatched values type, an uninitialized library, or a closing or closed service returns an error.

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

Only the following groups are supported. Registration and queries reject `"SECOND"`, `"MINUTE"`, and `"HOUR"`, even when explicitly cast to `GroupInterval`. Grouping is independent of `updateInterval`: uploads every 60 seconds or longer do not create finer-grained buckets.

| Constant | Example For `Get` |
| --- | --- |
| `GroupDay` | `2026-10-07` |
| `GroupWeek` | `2026-W41` |
| `GroupMonth` | `2026-10` |
| `GroupYear` | `2026` |
| `GroupForever` | `forever` |

All buckets use UTC. Weeks start on Monday and follow ISO week years. `GroupForever` ignores timestamps; its string form accepts `forever` or `FOREVER`. `Add` uses the current local call time, with no caller-supplied event timestamp.

### Expire Inactive Records

Set `Dimension.ClearAfter` to make persisted records eligible for deletion after that duration of inactivity. Zero disables expiry; negative durations are invalid. Positive durations round up to whole seconds.

Every insert or update stores `expire_time = last_update_time + ClearAfter` in SQL, using Unix seconds and the retained latest addition timestamp. With `ClearAfter == 0`, it stores `expire_time = 0`, which means never expire. Delayed uploads do not move `last_update_time` backwards or extend expiry merely because they were uploaded later.

One service-wide cleanup worker runs per process, even with no registered statistics. **There is no immediate startup cleanup.** Each successful initialization chooses an independent random first delay between **1 second and 24 hours**. After a sweep completes successfully, the worker waits **24 hours** before the next sweep, not until midnight. This staggers processes sharing the same tables but does not guarantee that cleaners never overlap. No cleanup runs while all service processes are stopped; restarting a process chooses a new delay, so frequent restarts can postpone cleanup.

Each sweep captures the application-server time once, using the same clock as `Add`, then deletes rows where `expire_time > 0` and `expire_time <= that cutoff` in batches of at most **500 rows**. Every batch commits independently. The worker continues until a successful batch deletes fewer than 500 rows; an exact multiple requires one final empty batch. There is no whole-sweep row or runtime limit. Rows becoming expired after the cutoff wait for a later sweep, and deletion rechecks expiry so refreshed rows are not removed merely because they were previously eligible. Batch-marker creation also uses application-server time; markers older than 30 days at the same sweep cutoff are pruned after record cleanup. Persisted expiry does not depend on re-registering the original dimension.

Expiry is **best-effort cleanup**, not a strict validity deadline. An expired row remains queryable until deleted. Daily sweeps can leave an expired row stored for nearly another day, or longer if cleanup fails. An update before deletion adds to the existing counter and refreshes expiry; an update after deletion creates a new counter. Both outcomes are intentional. Buffered rows can also recreate a deleted record.

Retention applies to every value and interval in the dimension, including `GroupForever`, and is based on inactivity rather than bucket age. A changed `ClearAfter` policy applies when a row is written again; registration does not rewrite existing deadlines. Keep application-server clocks synchronized across processes, and use consistent policies. Timestamps remain Unix seconds and buckets remain UTC; the database clock is not used.

Each deletion batch and marker pruning have separate **60-second SQL timeout contexts**; this is not a timeout for the whole sweep or a guarantee of immediate server-side statement cancellation. If either operation fails, the worker waits a fresh random whole-second delay from **60 through 300 seconds**, inclusive, then retries with the same sweep cutoff. Consecutive failures follow the same rule without a retry limit; already committed deletions remain committed. Only successful record cleanup and marker pruning start the next 24-hour wait. Cleanup runs independently of upload workers, although SQL locks and database load can still affect uploads. `Close` promptly interrupts initial, daily, and retry waits, waits for an in-flight cleanup operation, and prevents further cleanup batches from starting.

### Handle Errors

Initialization, registration, lookup, input validation in `Add`, and query errors return directly to their callers. `Add` silently ignores additions when it observes a closing or closed service, as explained above.

Background failures are handled internally through upload and cleanup retries. They are not logged, stored, or returned by `Close`. The library does not expose shared error sentinels or a latest-error status. Closed-pool errors are treated like other database errors and do not automatically stop the service.

### Shut Down

Call `EasyStatistics.Close()` explicitly during application shutdown. It stops accepting new work, signals workers to drain, and waits for their remaining uploads. `Init` does not accept an application context, and application cancellation does not shut down the library.

| Operation | Behavior |
| --- | --- |
| `Close` | Stops admission and waits for workers to finish; returns nil after initialization |
| Concurrent `Close` | Calls for the same service wait for its workers to finish |
| Repeated `Close` | Returns nil while the current service remains closed; does not restart uploads |
| `Init` while running or draining | Returns an error |
| `Init` after completed `Close` | Creates a fresh service and statistics registry |

The service owns one internal context for schema setup, uploads, and cleanup. It remains alive throughout draining and is canceled only after workers finish. `Close` stops scheduling cleanup sweeps and waits for an in-flight cleanup operation without starting another batch, while statistics workers drain. Those workers finish pending and active snapshots in order through the same retrying submission logic used during normal operation. Failed mini-batch attempts are retried after a freshly randomized delay of **2 to 10 seconds**, with a fresh timeout per attempt. Confirmed mini-batches are not replayed.

There is **no overall shutdown deadline**. An unavailable server, invalid schema, or another persistent upload failure can make shutdown wait indefinitely. Closing the `*sql.DB` pool early violates the ownership contract: pending uploads keep retrying, so `Close` can wait indefinitely. Unuploaded data stays in memory and is lost when the process exits.

After initialization, `Close` returns nil once all workers have exited. This does not guarantee that every buffered update was persisted; background failures are not returned. Calling `Close` before initialization returns an error. Registrations, lookups, and queries reject a closing or closed service; `Add` instead returns nil and ignores the addition.

Each accepted query owns an independent **60-second SQL timeout context**. `Close` does not wait for caller queries or cancel them. Queries accepted before shutdown may finish after `Close` returns, subject to their own timeout; new queries are rejected once shutdown begins.

The library never closes or reconfigures your database pool. Stop and join producers, wait for `EasyStatistics.Close()` to return and all accepted caller queries to finish, then call `db.Close()`. Keep the process alive throughout draining. Reinitialization does not move accepted queries to the new service or its database pool. Shutdown waits for background uploads and in-flight cleanup operations; cleanup scheduling and retry waits stop promptly, but upload retries continue during draining. Shutdown does not wait for application query calls.

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

`Init` checks connectivity, creates missing tables, checks that required columns are readable, and ensures a nonunique B-tree expiry index whose leading column is `expire_time`. A suitable existing index is reused; otherwise it creates `easy_statistics_expire_idx`, including on an existing table. Index creation races between processes are rechecked against the database catalog. The account needs schema-metadata access, table/index creation permission when those objects are missing, and read/write/delete permissions during operation.

Schema setup retains its **60-second SQL timeout**. Creating an index on a large existing table can take longer or block concurrent work; precreate a suitable nonunique expiry index during planned database maintenance before upgrading. Initialization returns an error if the index cannot be created or verified; it does not drop conflicting or invalid indexes automatically.

The counter table requires the `expire_time` column. Existing tables created without it must be updated separately before initialization. Apart from ensuring the expiry index, the library does not migrate existing columns or fully validate their types and constraints.

The Oracle insert explicitly stores an empty dimension value as a non-null empty CLOB rather than SQL NULL.

Native upsert or `MERGE` support is required. `SQLGaussDB` uses the openGauss-compatible `ON DUPLICATE KEY UPDATE` syntax with `EXCLUDED` references. Keep `record_id` as the counter table's only unique key: additional unique constraints can make MySQL-family or GaussDB upserts match a different logical record.

SQL Server casts generated ASCII record and batch key parameters to the schema's matching `VARCHAR` widths. This keeps Unicode parameter bindings from converting indexed key columns and forcing scans. With `go-mssqldb`, leave `connection timeout=0` and use SQL contexts for operation deadlines; use its separate `dial timeout` to bound connection establishment.

SQL dialect support is implemented for the databases above; it is not a claim that every driver or server version has been live-tested. SQLite and local MySQL 8.4.11 with `go-sql-driver/mysql` 1.10.1 were exercised during development. MySQL checks used isolated tables and covered transactions, retries, expiry cleanup, and the public API. Validate your chosen driver and deployment before production use.

SQL Server 2022 CU27 (16.0.4295.3) with `go-mssqldb` 1.11.2 was also exercised against a real local engine, including transaction rollback, expiry cleanup, lost-successful-reply recovery, and first-write/replay campaigns up to eight processes and 512 Add callers. The engine ran through Rosetta on Apple Silicon, which Microsoft does not support; these checks do not certify native deployment performance.

## API Reference

```text
Init(database *sql.DB, dialect SQLDialect) error
NewStatistics[T any](name string, updateInterval time.Duration, dimensions []Dimension) (*Statistics[T], error)
GetStatistics[T any](name string) (*Statistics[T], error)
Close() error

(*Statistics[T]).Add(values T, delta int64) error
(*Statistics[T]).Get(orderFields []OrderField, interval GroupInterval, groupedTime string) (*Record, error)
(*Statistics[T]).GetWithTime(orderFields []OrderField, interval GroupInterval, timestamp time.Time) (*Record, error)
(*Statistics[T]).GetWithUnixTimeSecond(orderFields []OrderField, interval GroupInterval, timestamp int64) (*Record, error)
```

Only one service can be running or draining at a time. After `Close` completes, `Init` may create a fresh service using the same or a different database pool. A failed initialization may be retried. Old statistics handles remain closed; re-register definitions after reinitialization or a process restart. Persisted counters remain in their original database. Statistic names must be unique within the current service and contain 1 to 255 UTF-8 bytes, with no surrounding whitespace or control characters. Field names follow the stricter `stat` tag rules above. Configuration slices are copied at registration.

## Design Notes

Dialect helpers in [dialect.go](dialect.go) generate SQL using only `SQLDialect`; they do not depend on the database backend. [sql.go](sql.go) owns SQL execution, transactions, batch identity, timeouts, and upload retries. [statistics.go](statistics.go) owns struct-field validation, typed input, buffering, the normal update interval, and final draining. [store.go](store.go) owns the service lifecycle, typed name lookup, and global cleanup worker.

`Statistics[T]` is a thin typed handle over a non-generic counter core. Participating tag names and their original struct-field indexes are cached at registration; `Add` reads only those fields through reflection before the existing normalization and buffering steps. This preserves the correct name-to-value mapping when other fields are skipped. The SQL backend and persisted identity format do not depend on the Go values type. This improves caller-side type checking without claiming a performance improvement.

`runCleanup` owns cleanup scheduling, the fixed application-time cutoff for each sweep, the repeat-until-short-batch loop, failure handling, and shutdown checks. The SQL backend's `cleanup` method attempts exactly one deletion batch and returns its affected-row count and error; it does not schedule, repeat, or inspect the service's closing signal. Old batch-marker pruning is a separate SQL operation called by the worker.

### Buffering And Retries

Each statistic has its own independently locked active buffer shards, pending batch, update interval, and worker. The `shardNum` constant in [statistics.go](statistics.go) defaults to **8** and applies to every statistic. It is a source-level setting, not a runtime option; use a positive value. Setting it to `1` uses one active buffer.

Active maps use structured keys containing the encoded dimension fields, encoded values, interval, and bucket instead of generated record IDs. Each key is routed with `shardIndex = hash(dimension, value, interval, bucket) % shardNum`, using 64-bit FNV-1a. The same key always maps to the same shard within its statistic, so a record has at most one active entry. Hash collisions share a shard, not a map entry; the full structured key still identifies the record.

Input normalization, value escaping, time-bucket calculation, and shard selection happen before locking. An `Add` can affect several shards. It acquires the affected locks in index order to avoid deadlocks, checks the closing signal before applying any changes, and releases the locks in reverse order.

`Add` uses a local buffer for up to eight counter changes. Definitions producing more changes use a heap-allocated buffer; eight is not a limit on configured dimensions or intervals.

At an update, the worker replaces each shard's active map with an empty one under that shard's lock. Outside those locks, it generates the existing record ID once per detached row when creating the pending batch. No cross-shard contribution merge is needed. SQL work also runs outside shard locks. Record IDs and counter identities remain unchanged; SQL rows additionally store their expiry deadline.

Different keys can use different shard locks, but updates to one hot key always serialize on its owning shard. More shards can distribute different keys more widely; they do not spread one key across multiple locks. A pending batch can retain an earlier contribution while newer additions to that key accumulate in its active shard.

The shard mutexes are spaced 128 bytes apart on 64-bit builds. This reduces false sharing on CPUs with common 64-byte or 128-byte cache lines without changing routing or locking behavior. With eight shards, the padding adds 896 bytes per statistic. Its performance benefit depends on the CPU and caller concurrency; it does not eliminate contention within a shard and is not a guaranteed speedup on every server.

Deferring record IDs avoids repeated JSON encoding and hashing when many additions update the same records. With mostly unique records, that work moves to batch preparation rather than disappearing. Conversion to SQL rows also adds batch-preparation allocations, and structured active keys can use more memory.

The SQL backend splits a pending snapshot into mini-batches of at most **500 rows**, controlled by the positive `miniBatchSize` constant in [sql.go](sql.go). Mini-batches are submitted sequentially, with one transaction per mini-batch. Confirmed rows are removed from the pending snapshot before proceeding; the current mini-batch is retained unchanged across failures. Newer snapshots do not overtake pending rows. Atomicity is per mini-batch, not per complete snapshot.

Every dialect uses the same upload flow: register the batch marker, execute one native SQL upsert per record, then commit. SQL adds the incoming counter delta, retains the latest timestamp, and recalculates expiry; existing key metadata is not overwritten. Uploads trust the generated record ID and library table schema without reading each record back. Queries still validate stored key metadata. These are sequential single-row upserts, not one bulk statement.

Record IDs are sorted within each mini-batch to give writers a consistent lock order. Concurrent servers can still wait on overlapping rows, and transaction locks remain held until commit or rollback, not just until each row statement finishes. Upserts avoid the previous missing-row locking read but do not guarantee freedom from deadlocks or lock timeouts; those failures use the same safe batch retry path.

Failed attempts retry inside the SQL backend after a random whole-second delay from **2 through 10 seconds**, inclusive, until successful or the internal context is canceled. Each failure chooses a new delay to spread retries from concurrent workers and processes; this does not guarantee that retries never overlap. This retry jitter is independent of randomized cleanup scheduling. `Close` leaves in-flight submissions and retry waits running with the same internal context, then drains newer active rows. Successful snapshot submissions restart the statistic's normal update interval. Closed pools and unreachable database servers use the same retry path.

The SQL backend gives schema setup, each cleanup operation, and each mini-batch attempt a **60-second child context** of the internal service context. Queries use their own independent 60-second contexts. Cleanup is independent of uploads and cannot consume a mini-batch's write timeout. Each following mini-batch and every retry receives a fresh budget; there is no overall snapshot deadline. Timeouts require driver cancellation support to be effective.

Uploads perform no retention cleanup. The single service-wide cleanup worker uses `cleanupInterval` in [store.go](store.go), set to 24 hours. It deletes expired records directly with a SQL predicate instead of selecting IDs for later deletion. Multiple service processes may run sweeps against the same database; deletion always checks the currently stored deadline.

### Storage And Batch Identity

`easy_statistics` stores counters, their identities, and `expire_time`. Ordered field names and normalized values occupy separate, URL-escaped text columns. SHA-256 keys identify dimensions and records without relying on database text collations. SQL timestamps use Unix seconds; delayed uploads never lower a record's last-update timestamp. Expiry is recalculated from that retained timestamp and the incoming row's `ClearAfter` policy, with zero reserved for non-expiring rows.

`easy_statistics_batches` stores a random 256-bit batch ID and an application-generated creation timestamp in Unix seconds. The SQL backend creates a distinct ID for each mini-batch before its first database attempt and retains it across retries. That mini-batch's counter changes and marker commit in one transaction. Retrying an uncertain commit checks the same ID, so a retained marker prevents reapplying its counters. After confirmation, the marker is deleted; a failed deletion does not reapply confirmed changes or advance to the next mini-batch.

Markers older than **30 days** are pruned by the global cleanup worker. Removing a confirmed mini-batch's marker remains part of its submission, so confirmation can advance to the next mini-batch. For large counter tables, an index on `expire_time` can speed up expiry; `Init` reuses a suitable existing index or creates one automatically.

### Reliability Limits

This library is for **approximate statistics**, not a durable event ledger:

- A crash loses buffered additions because buffers exist only in memory.
- Shutdown may wait indefinitely for a persistent database failure, including a pool closed before pending uploads finish.
- `Add` ignores additions when it observes shutdown and returns nil; stop producers before shutdown to avoid dropping late additions.
- Expiry is opportunistic: expired rows remain visible and can be refreshed until a cleanup sweep deletes them.
- Mini-batches commit independently, so queries can observe a partially uploaded snapshot.
- An unavailable database can increase memory use as new values accumulate.
- An uncertain batch resumed after its marker expires may be counted again.
- Counter arithmetic can overflow if callers allow totals outside the `int64` range.

SQL transactions protect shared counter updates across processes, but RAM buffering and finite marker retention are not an unlimited exactly-once guarantee.