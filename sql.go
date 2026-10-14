package EasyStatistics

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	mathrand "math/rand"
	"sort"
	"strings"
	"time"
)

const (
	miniBatchSize    = 500
	cleanupBatchSize = 500
	sqlTimeout       = 60 * time.Second
	batchRetention   = 30 * 24 * time.Hour
	// Dimension encodings are ASCII. Keep scalar Oracle binds below VARCHAR2
	// limits even when the supplied driver expands each character to two bytes.
	oracleTextChunkSize = 1000
)

type counterRow struct {
	id         string
	name       string
	dimension  string
	value      string
	interval   GroupInterval
	bucket     int64
	updated    int64
	counter    int64
	clearAfter time.Duration
}

type uploadBatch struct {
	rows    map[string]counterRow
	current *miniBatch
}

type miniBatch struct {
	id      string
	rows    []counterRow
	applied bool
}

type sqlBackend struct {
	db      *sql.DB
	dialect SQLDialect
	schema  []string
}

func newSQLBackend(database *sql.DB, dialect SQLDialect) (*sqlBackend, error) {
	if database == nil {
		return nil, errors.New("SQL database is required")
	}
	schema, err := schemaFor(dialect)
	if err != nil {
		return nil, err
	}

	return &sqlBackend{db: database, dialect: dialect, schema: schema}, nil
}

func (backend *sqlBackend) ensureSchema(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, sqlTimeout)
	defer cancel()
	if err := backend.db.PingContext(ctx); err != nil {
		return fmt.Errorf("check SQL connection: %w", err)
	}
	for index, statement := range backend.schema {
		if backend.schemaReady(ctx, index) == nil {
			continue
		}
		for attempt := 0; ; attempt++ {
			_, err := backend.db.ExecContext(ctx, statement)
			if err == nil {
				break
			}
			var sqlState interface{ SQLState() string }
			retryable := errors.As(err, &sqlState) && (sqlState.SQLState() == "23505" || sqlState.SQLState() == "42P07" || sqlState.SQLState() == "42710")
			if attempt == 4 || !retryable {
				return fmt.Errorf("create statistics schema: %w", err)
			}
			if err := waitDelay(ctx, 200*time.Millisecond); err != nil {
				return err
			}
		}
		if err := backend.schemaReady(ctx, index); err != nil {
			return fmt.Errorf("validate statistics schema: %w", err)
		}
	}
	return backend.ensureExpiryIndex(ctx)
}

func (backend *sqlBackend) ensureExpiryIndex(ctx context.Context) error {
	query := backend.dialect.expiryIndexQuery()
	var count int
	if err := backend.db.QueryRowContext(ctx, query).Scan(&count); err != nil {
		return fmt.Errorf("check statistics expiry index: %w", err)
	}
	if count > 0 {
		return nil
	}
	_, createError := backend.db.ExecContext(ctx, backend.dialect.createExpiryIndex())
	for {
		if err := backend.db.QueryRowContext(ctx, query).Scan(&count); err != nil {
			return fmt.Errorf("validate statistics expiry index: %w", errors.Join(createError, err))
		}
		if count > 0 {
			return nil
		}
		if createError == nil {
			return errors.New("statistics expiry index was not created")
		}
		// TiDB can reject a competing CREATE INDEX before the first process's
		// asynchronous DDL job publishes its index in the catalog. A single
		// immediate recheck is not sufficient, even on an empty counter table.
		// Only this explicitly pending-job error is waited on; a conflicting,
		// unsuitable existing index or a permissions error must still fail.
		if backend.dialect != SQLTiDB || !strings.Contains(createError.Error(), "a background job is trying to add the same index") {
			return fmt.Errorf("create statistics expiry index: %w", createError)
		}
		if err := waitDelay(ctx, 200*time.Millisecond); err != nil {
			return fmt.Errorf("wait for concurrent statistics expiry index: %w", errors.Join(createError, err))
		}
	}
}

func (backend *sqlBackend) schemaReady(ctx context.Context, index int) error {
	rows, err := backend.db.QueryContext(ctx, backend.dialect.schemaQuery(index))
	if err != nil {
		return err
	}
	for rows.Next() {
	}
	return errors.Join(rows.Err(), rows.Close())
}

func waitDelay(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (backend *sqlBackend) getRows(ctx context.Context, identities []string) (map[string]counterRow, error) {
	ctx, cancel := context.WithTimeout(ctx, sqlTimeout)
	defer cancel()
	parameters := make([]any, len(identities))
	requested := make(map[string]bool, len(identities))
	for index, identity := range identities {
		parameters[index] = identity
		requested[identity] = true
	}
	rows, err := backend.db.QueryContext(ctx, backend.dialect.selectRecords(len(identities)), parameters...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make(map[string]counterRow, len(identities))
	for rows.Next() {
		var row counterRow
		var value sql.NullString
		var valueIsNull int
		targets := []any{
			&row.id, &row.name, &row.dimension, &value, &row.interval, &row.bucket, &row.updated, &row.counter,
		}
		if backend.dialect == SQLOracle {
			targets = append(targets, &valueIsNull)
		}
		if err := rows.Scan(targets...); err != nil {
			return nil, err
		}
		if valueIsNull != 0 || (!value.Valid && backend.dialect != SQLOracle) {
			return nil, errors.New("stored dimension value is NULL")
		}
		if !requested[row.id] {
			return nil, errors.New("stored record was not requested")
		}
		if _, exists := result[row.id]; exists {
			return nil, errors.New("stored record ID is duplicated")
		}
		row.value = value.String
		if backend.dialect == SQLGaussDB && row.value == gaussEmptyValue {
			row.value = ""
		}
		result[row.id] = row
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, err
	}
	return result, nil
}

func (backend *sqlBackend) count(ctx context.Context) ([]DimensionCount, int64, error) {
	ctx, cancel := context.WithTimeout(ctx, sqlTimeout)
	defer cancel()
	rows, err := backend.db.QueryContext(ctx, backend.dialect.countRecords())
	if err != nil {
		return nil, 0, fmt.Errorf("count stored statistics records: %w", err)
	}
	defer rows.Close()
	counts := make([]DimensionCount, 0)
	var totalDBCount int64
	for rows.Next() {
		var counted DimensionCount
		var encodedFields string
		if err := rows.Scan(&counted.Name, &encodedFields, &counted.Count); err != nil {
			return nil, 0, fmt.Errorf("read statistics count: %w", err)
		}
		if err := validateIdentifier(counted.Name); err != nil {
			return nil, 0, fmt.Errorf("stored statistics name: %w", err)
		}
		if len(encodedFields) > MaxDimensionFieldsBytes {
			return nil, 0, errors.New("stored dimension_fields exceeds its byte limit")
		}
		counted.OrderFields = strings.Split(encodedFields, ":")
		for _, field := range counted.OrderFields {
			if !validStatName(field) {
				return nil, 0, fmt.Errorf("stored dimension field %q is invalid", field)
			}
		}
		counts = append(counts, counted)
		totalDBCount += counted.Count
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, 0, fmt.Errorf("read statistics counts: %w", err)
	}
	sort.Slice(counts, func(left, right int) bool {
		if counts[left].Name != counts[right].Name {
			return counts[left].Name < counts[right].Name
		}
		return strings.Join(counts[left].OrderFields, ":") < strings.Join(counts[right].OrderFields, ":")
	})
	return counts, totalDBCount, nil
}

func (backend *sqlBackend) countBatches(ctx context.Context) (int64, error) {
	ctx, cancel := context.WithTimeout(ctx, sqlTimeout)
	defer cancel()
	var count int64
	if err := backend.db.QueryRowContext(ctx, backend.dialect.countBatches()).Scan(&count); err != nil {
		return 0, fmt.Errorf("count stored statistics batches: %w", err)
	}
	return count, nil
}

func expireTime(updated int64, clearAfter time.Duration) int64 {
	if clearAfter == 0 {
		return 0
	}
	seconds := int64(clearAfter / time.Second)
	if clearAfter%time.Second != 0 {
		seconds++
	}
	return updated + seconds
}

func (backend *sqlBackend) applyBatch(ctx context.Context, batch *miniBatch) error {
	statement, err := backend.dialect.upsertRecord()
	if err != nil {
		return err
	}
	transaction, err := backend.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin statistics batch: %w", err)
	}
	defer transaction.Rollback()
	var confirmed string
	err = transaction.QueryRowContext(ctx, backend.dialect.selectBatch(), batch.id).Scan(&confirmed)
	if err == nil {
		return nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("check statistics batch: %w", err)
	}
	if _, err := transaction.ExecContext(ctx, backend.dialect.insertBatch(), batch.id, time.Now().UTC().Unix()); err != nil {
		return fmt.Errorf("register statistics batch: %w", err)
	}
	var marker string
	if backend.dialect == SQLOracle {
		marker = "!" + batch.id
	}
	for _, incoming := range batch.rows {
		value := incoming.value
		if backend.dialect == SQLOracle && len(value) > oracleTextChunkSize {
			value = marker
		}
		_, err := transaction.ExecContext(ctx, statement, incoming.id, incoming.name, incoming.dimension, value,
			string(incoming.interval), incoming.bucket, incoming.updated, expireTime(incoming.updated, incoming.clearAfter), incoming.counter, expireTime(0, incoming.clearAfter))
		if err != nil {
			return fmt.Errorf("upsert statistics record: %w", err)
		}
		if backend.dialect == SQLOracle {
			if err := backend.completeOracleClob(ctx, transaction, incoming, marker); err != nil {
				return err
			}
		}
	}
	if err := transaction.Commit(); err != nil {
		return fmt.Errorf("commit statistics batch: %w", err)
	}
	return nil
}

func (backend *sqlBackend) completeOracleClob(ctx context.Context, transaction *sql.Tx, row counterRow, marker string) error {
	if len(row.value) <= oracleTextChunkSize {
		return nil
	}
	result, err := transaction.ExecContext(ctx, backend.dialect.startOracleClob(), row.value[:oracleTextChunkSize], row.id, marker)
	if err != nil {
		return fmt.Errorf("start statistics dimension_value CLOB: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("count statistics dimension_value CLOB start: %w", err)
	}
	if affected == 0 {
		return nil
	}
	if affected != 1 {
		return fmt.Errorf("start statistics dimension_value CLOB affected %d rows, expected one", affected)
	}
	statement := backend.dialect.appendOracleClob()
	for offset := oracleTextChunkSize; offset < len(row.value); offset += oracleTextChunkSize {
		end := offset + oracleTextChunkSize
		if end > len(row.value) {
			end = len(row.value)
		}
		result, err := transaction.ExecContext(ctx, statement, row.value[offset:end], row.id, int64(offset))
		if err != nil {
			return fmt.Errorf("complete statistics dimension_value CLOB: %w", err)
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("count statistics dimension_value CLOB update: %w", err)
		}
		if affected != 1 {
			return fmt.Errorf("complete statistics dimension_value CLOB affected %d rows, expected one", affected)
		}
	}
	return nil
}

func (backend *sqlBackend) submit(parent context.Context, batch *uploadBatch) error {
	for {
		if err := parent.Err(); err != nil {
			return err
		}
		if batch == nil || len(batch.rows) == 0 {
			return nil
		}
		ctx, cancel := context.WithTimeout(parent, sqlTimeout)
		err := backend.submitOnce(ctx, batch)
		cancel()
		if err == nil {
			continue
		}
		retryDelay := time.Duration(2+mathrand.Intn(9)) * time.Second
		if err := waitDelay(parent, retryDelay); err != nil {
			return err
		}
	}
}

func (backend *sqlBackend) submitOnce(ctx context.Context, batch *uploadBatch) error {
	if batch.current == nil {
		rowCount := len(batch.rows)
		if rowCount > miniBatchSize {
			rowCount = miniBatchSize
		}
		rows := make([]counterRow, 0, rowCount)
		for _, row := range batch.rows {
			rows = append(rows, row)
			if len(rows) == rowCount {
				break
			}
		}
		sort.Slice(rows, func(first, second int) bool { return rows[first].id < rows[second].id })
		batch.current = &miniBatch{rows: rows}
	}
	current := batch.current
	if current.id == "" {
		var random [32]byte
		if _, err := rand.Read(random[:]); err != nil {
			return fmt.Errorf("create batch identity: %w", err)
		}
		current.id = hex.EncodeToString(random[:])
	}
	if !current.applied {
		if err := backend.applyBatch(ctx, current); err != nil {
			return err
		}
		current.applied = true
	}
	if _, err := backend.db.ExecContext(ctx, backend.dialect.deleteBatch(), current.id); err != nil {
		return fmt.Errorf("clean confirmed statistics batch: %w", err)
	}
	for _, row := range current.rows {
		delete(batch.rows, row.id)
	}
	batch.current = nil
	return nil
}

func (backend *sqlBackend) cleanup(parent context.Context, cutoff int64) (int64, error) {
	ctx, cancel := context.WithTimeout(parent, sqlTimeout)
	defer cancel()
	result, err := backend.db.ExecContext(ctx, backend.dialect.pruneRecords(), cutoff)
	if err != nil {
		return 0, fmt.Errorf("remove expired statistics records: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("count removed statistics records: %w", err)
	}
	return affected, nil
}

func (backend *sqlBackend) cleanupBatches(parent context.Context, cutoff int64) error {
	ctx, cancel := context.WithTimeout(parent, sqlTimeout)
	defer cancel()
	if _, err := backend.db.ExecContext(ctx, backend.dialect.pruneBatches(), cutoff); err != nil {
		return fmt.Errorf("remove expired statistics batches: %w", err)
	}
	return nil
}
