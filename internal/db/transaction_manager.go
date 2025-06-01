package db

import (
	"context"
	"database/sql"
	"sync"
	"sync/atomic"
)

type TransactionManager struct {
	Db        *sql.DB
	Tx        *sql.Tx
	Count     atomic.Int64
	mu        sync.Mutex
	batchSize int64
}

func NewTransactionManager(ctx context.Context, db *sql.DB, batchSize int64) (*TransactionManager, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}

	return &TransactionManager{
		Db:        db,
		Tx:        tx,
		batchSize: batchSize,
	}, nil

}

func (m *TransactionManager) AddPgcr(ctx context.Context, instanceId int64, blob []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, err := m.Tx.ExecContext(ctx, `INSERT INTO raid_pgcr (instance_id, blobl) VALUES ($1, $2)`, instanceId, blob); err != nil {
		return err
	}

	if newCount := m.Count.Add(1); newCount >= m.batchSize {
		if err := m.Tx.Commit(); err != nil {
			return err
		}

		m.Count.Store(0)
		tx2, err := m.Db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}

		m.Tx = tx2
	}
	return nil
}

func (m *TransactionManager) Close(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.Count.Load() > 0 {
		if err := m.Tx.Commit(); err != nil {
			return err
		}
		m.Count.Store(0)
	}
	return nil
}
