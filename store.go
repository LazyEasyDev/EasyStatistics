package EasyStatistics

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"
)

const cleanupInterval = 24 * time.Hour

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
	closing    chan struct{}
	workers    sync.WaitGroup
}

func Init(database *sql.DB, dialect SQLDialect, onError func(error)) error {
	defaultServiceMu.RLock()
	alreadyInitialized := defaultService != nil && defaultService.ctx.Err() == nil
	defaultServiceMu.RUnlock()
	if alreadyInitialized {
		return errors.New("EasyStatistics is already initialized")
	}
	backend, err := newSQLBackend(database, dialect)
	if err != nil {
		return err
	}
	initialized := newService(backend, onError)
	if err := backend.ensureSchema(initialized.ctx); err != nil {
		initialized.cancel()
		return err
	}
	defaultServiceMu.Lock()
	if defaultService != nil && defaultService.ctx.Err() == nil {
		defaultServiceMu.Unlock()
		initialized.cancel()
		return errors.New("EasyStatistics is already initialized")
	}
	initialized.workers.Add(1)
	defaultService = initialized
	defaultServiceMu.Unlock()
	go initialized.runCleanup()
	return nil
}

func newService(backend *sqlBackend, onError func(error)) *service {
	ctx, cancel := context.WithCancel(context.Background())
	return &service{
		backend: backend, ctx: ctx, cancel: cancel, onError: onError,
		statistics: make(map[string]*Statistics), closing: make(chan struct{}),
	}
}

func (initialized *service) runCleanup() {
	defer initialized.workers.Done()
	ticker := time.NewTicker(cleanupInterval)
	defer ticker.Stop()
	for {
		if initialized.isClosing() {
			return
		}
		if err := initialized.backend.cleanup(initialized.ctx); err != nil {
			if initialized.onError != nil {
				initialized.onError(err)
			}
			if isClosedDatabase(err) {
				initialized.stop()
				return
			}
		}
		select {
		case <-initialized.closing:
			return
		case <-ticker.C:
		}
	}
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
	if initialized.isClosing() {
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
	initialized.close()
	return nil
}

func (initialized *service) isClosing() bool {
	select {
	case <-initialized.closing:
		return true
	default:
		return false
	}
}

func (initialized *service) stop() {
	initialized.mu.Lock()
	if !initialized.isClosing() {
		close(initialized.closing)
	}
	initialized.mu.Unlock()
}

func (initialized *service) close() {
	initialized.stop()
	initialized.workers.Wait()
	initialized.cancel()
}
