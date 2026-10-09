package EasyStatistics

import (
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"fmt"
	"math/big"
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
	mu         sync.Mutex
	statistics map[string]*Statistics
	closing    chan struct{}
	workers    sync.WaitGroup
}

func Init(database *sql.DB, dialect SQLDialect) error {
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
	cleanupDelay, err := randomCleanupDelay()
	if err != nil {
		return err
	}
	initialized := newService(backend)
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
	go initialized.runCleanup(cleanupDelay)
	return nil
}

func newService(backend *sqlBackend) *service {
	ctx, cancel := context.WithCancel(context.Background())
	return &service{
		backend: backend, ctx: ctx, cancel: cancel,
		statistics: make(map[string]*Statistics), closing: make(chan struct{}),
	}
}

func randomCleanupDelay() (time.Duration, error) {
	seconds, err := rand.Int(rand.Reader, big.NewInt(int64(cleanupInterval/time.Second)))
	if err != nil {
		return 0, fmt.Errorf("randomize statistics cleanup schedule: %w", err)
	}
	return time.Duration(seconds.Int64()+1) * time.Second, nil
}

func (initialized *service) runCleanup(initialDelay time.Duration) {
	defer initialized.workers.Done()
	timer := time.NewTimer(initialDelay)
	defer timer.Stop()
	select {
	case <-initialized.closing:
		return
	case <-timer.C:
	}
	ticker := time.NewTicker(cleanupInterval)
	defer ticker.Stop()
	for {
		cutoff := time.Now().UTC().Unix()
		for {
			if initialized.isClosing() {
				return
			}
			affected, err := initialized.backend.cleanup(initialized.ctx, cutoff)
			if err != nil {
				if isClosedDatabase(err) {
					initialized.stop()
					return
				}
				break
			}
			if affected < 0 || affected > cleanupBatchSize {
				break
			}
			if affected < cleanupBatchSize {
				if initialized.isClosing() {
					return
				}
				err = initialized.backend.cleanupBatches(initialized.ctx, cutoff-int64(batchRetention/time.Second))
				if err != nil {
					if isClosedDatabase(err) {
						initialized.stop()
						return
					}
				}
				break
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
