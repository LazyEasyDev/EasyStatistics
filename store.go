package EasyStatistics

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	defaultServiceMu sync.RWMutex
	defaultService   *service
)

type service struct {
	backend    *sqlBackend
	ctx        context.Context
	cancel     context.CancelFunc
	onError    func(error)
	mu         sync.Mutex
	statistics map[string]*Statistics
	closed     bool
	workers    sync.WaitGroup
}

func Init(ctx context.Context, database *sql.DB, dialect SQLDialect, onError func(error)) error {
	if ctx == nil {
		return errors.New("context is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	defaultServiceMu.RLock()
	alreadyInitialized := defaultService != nil
	defaultServiceMu.RUnlock()
	if alreadyInitialized {
		return errors.New("EasyStatistics is already initialized")
	}
	backend, err := newSQLBackend(database, dialect)
	if err != nil {
		return err
	}
	if err := backend.ensureSchema(ctx); err != nil {
		return err
	}
	initialized, err := newService(ctx, backend, onError)
	if err != nil {
		return err
	}
	defaultServiceMu.Lock()
	if defaultService != nil {
		defaultServiceMu.Unlock()
		initialized.cancel()
		return errors.New("EasyStatistics is already initialized")
	}
	defaultService = initialized
	defaultServiceMu.Unlock()
	go initialized.wait()
	return nil
}

func newService(parent context.Context, backend *sqlBackend, onError func(error)) (*service, error) {
	if parent == nil {
		return nil, errors.New("context is required")
	}
	if err := parent.Err(); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(parent)
	return &service{
		backend: backend, ctx: ctx, cancel: cancel, onError: onError,
		statistics: make(map[string]*Statistics),
	}, nil
}

func currentService() (*service, error) {
	defaultServiceMu.RLock()
	initialized := defaultService
	defaultServiceMu.RUnlock()
	if initialized == nil {
		return nil, errors.New("EasyStatistics is not initialized")
	}
	return initialized, nil
}

func NewStatistics(name string, updateInterval time.Duration, fields []string, dimensions []Dimension) (*Statistics, error) {
	initialized, err := currentService()
	if err != nil {
		return nil, err
	}
	return initialized.newStatistics(name, updateInterval, fields, dimensions)
}

func GetStatistics(name string) (*Statistics, error) {
	initialized, err := currentService()
	if err != nil {
		return nil, err
	}
	initialized.mu.Lock()
	statistic := initialized.statistics[name]
	initialized.mu.Unlock()
	if initialized.ctx.Err() != nil {
		return nil, errors.New("EasyStatistics is closed")
	}
	if statistic == nil {
		return nil, fmt.Errorf("statistics %q does not exist", name)
	}
	return statistic, nil
}

func Close() error {
	initialized, err := currentService()
	if err != nil {
		return err
	}
	return initialized.close()
}

func Wait() error {
	initialized, err := currentService()
	if err != nil {
		return err
	}
	return initialized.wait()
}

func (initialized *service) wait() error {
	<-initialized.ctx.Done()
	return initialized.close()
}

func (initialized *service) close() error {
	initialized.mu.Lock()
	if initialized.closed {
		initialized.mu.Unlock()
		return nil
	}
	initialized.cancel()
	initialized.mu.Unlock()

	initialized.workers.Wait()

	initialized.mu.Lock()
	initialized.closed = true
	initialized.mu.Unlock()
	return nil
}
