package EasyStatistics

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"time"
)

const batchRetention = 30 * 24 * time.Hour

type counterRow struct {
	id          string
	name        string
	dimensionID string
	dimension   string
	value       string
	interval    GroupInterval
	bucket      int64
	updated     int64
	counter     int64
}

type uploadBatch struct {
	id      string
	rows    map[string]counterRow
	applied bool
}

type sqlBackend struct {
	db      *sql.DB
	dialect SQLDialect
	schema  []string
}

type rowQueryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
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
	return nil
}

func (backend *sqlBackend) schemaReady(ctx context.Context, index int) error {
	rows, err := backend.db.QueryContext(ctx, backend.schemaQuery(index))
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

func (backend *sqlBackend) readRow(ctx context.Context, queryer rowQueryer, identity string, locked bool) (counterRow, error) {
	row := counterRow{id: identity}
	err := queryer.QueryRowContext(ctx, backend.selectRecord(locked), identity).Scan(
		&row.name, &row.dimensionID, &row.dimension, &row.value, &row.interval, &row.bucket, &row.updated, &row.counter,
	)
	return row, err
}

func (backend *sqlBackend) applyBatch(ctx context.Context, batch *uploadBatch) error {
	rows := make([]counterRow, 0, len(batch.rows))
	for _, row := range batch.rows {
		rows = append(rows, row)
	}
	sort.Slice(rows, func(first, second int) bool { return rows[first].id < rows[second].id })
	transaction, err := backend.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin statistics batch: %w", err)
	}
	defer transaction.Rollback()
	var confirmed string
	err = transaction.QueryRowContext(ctx, backend.selectBatch(), batch.id).Scan(&confirmed)
	if err == nil {
		return nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("check statistics batch: %w", err)
	}
	if _, err := transaction.ExecContext(ctx, backend.insertBatch(), batch.id); err != nil {
		return fmt.Errorf("register statistics batch: %w", err)
	}
	for _, incoming := range rows {
		existing, err := backend.readRow(ctx, transaction, incoming.id, true)
		if errors.Is(err, sql.ErrNoRows) {
			_, err = transaction.ExecContext(ctx, backend.insertRecord(), incoming.id, incoming.name, incoming.dimensionID, incoming.dimension, incoming.value,
				string(incoming.interval), incoming.bucket, incoming.updated, incoming.counter)
		} else if err == nil {
			if existing.name != incoming.name || existing.dimensionID != incoming.dimensionID || existing.dimension != incoming.dimension || existing.value != incoming.value || existing.interval != incoming.interval || existing.bucket != incoming.bucket {
				return errors.New("stored record identity does not match its key")
			}
			counter := existing.counter + incoming.counter
			updated := incoming.updated
			if existing.updated > updated {
				updated = existing.updated
			}
			_, err = transaction.ExecContext(ctx, backend.updateRecord(), counter, updated, incoming.id)
		}
		if err != nil {
			return fmt.Errorf("write statistics record: %w", err)
		}
	}
	if err := transaction.Commit(); err != nil {
		return fmt.Errorf("commit statistics batch: %w", err)
	}
	return nil
}

func (backend *sqlBackend) submit(ctx context.Context, batch *uploadBatch) error {
	if !batch.applied {
		if err := backend.applyBatch(ctx, batch); err != nil {
			return err
		}
		batch.applied = true
	}
	if _, err := backend.db.ExecContext(ctx, backend.deleteBatch(), batch.id); err != nil {
		return fmt.Errorf("clean confirmed statistics batch: %w", err)
	}
	return nil
}

func (backend *sqlBackend) cleanupBatches(ctx context.Context) error {
	if _, err := backend.db.ExecContext(ctx, backend.pruneBatches(), int64(batchRetention/time.Second)); err != nil {
		return fmt.Errorf("remove expired statistics batches: %w", err)
	}
	return nil
}

func (backend *sqlBackend) cleanupDimension(ctx context.Context, name string, dimension Dimension) error {
	if dimension.ClearAfter <= 0 {
		return nil
	}
	seconds := int64(dimension.ClearAfter / time.Second)
	if dimension.ClearAfter%time.Second != 0 {
		seconds++
	}
	fields := encodeSequence(dimension.OrderFields)
	if _, err := backend.db.ExecContext(ctx, backend.pruneRecords(), dimensionID(name, fields), seconds); err != nil {
		return fmt.Errorf("remove expired statistics %q dimension %q: %w", name, fields, err)
	}
	return nil
}
