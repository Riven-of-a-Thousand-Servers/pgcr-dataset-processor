package db

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"sync"
	"sync/atomic"

	"pgcr-dataset-processor/internal/config"
)

type SqlArgs struct {
	Query string
	Args  []string
}

type Stats struct {
	Rollbacks    atomic.Int64
	CommitErrors atomic.Int64
	Commits      atomic.Int64
}

type TransactionManager struct {
	Db        *sql.DB
	Tx        *sql.Tx
	Ctx       context.Context
	Count     atomic.Int64
	Stats     Stats
	input     chan SqlArgs
	batchSize int64
}

func Connect(datasource config.Datasource) (*sql.DB, error) {
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		datasource.Host, datasource.Port, datasource.User, datasource.Password, datasource.Database)
	log.Print("Connecting to postgres db...")
	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		return nil, fmt.Errorf("Error opening connection to database: %v", err)
	}
	return db, nil
}

func NewTransactionManager(ctx context.Context, db *sql.DB, wg *sync.WaitGroup, batchSize int64) (*TransactionManager, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}

	input := make(chan SqlArgs, 50)
	tm := &TransactionManager{
		Db:        db,
		Tx:        tx,
		Ctx:       ctx,
		batchSize: batchSize,
		input:     input,
	}

	go func() {
		defer wg.Done()
		tm.run()
	}()

	return tm, nil
}

func (tm *TransactionManager) Add(args SqlArgs) {
	tm.input <- args
}

func (tm *TransactionManager) run() {
	for {
		select {
		case <-tm.Ctx.Done():
			log.Printf("Transaction manager closing...")
			err := tm.Tx.Rollback()
			if err != nil {
				log.Printf("Error rolling back transaction: %v", err)
			}
			return
		case args := <-tm.input:
			_, err := tm.Tx.ExecContext(tm.Ctx, args.Query, args.Args)
			if err != nil {
				log.Panicf("Error adding to transaction: %v", err)
			}

			if count := tm.Count.Add(1); count >= tm.batchSize {
				if err := tm.Tx.Commit(); err != nil {
					tm.Stats.CommitErrors.Add(1)
					log.Printf("Commit error: %v", err)
					_ = tm.Tx.Rollback()
				} else {
					tm.Stats.Commits.Add(1)
					tm.Count.Store(0)
				}

				tx, err := tm.Db.BeginTx(tm.Ctx, nil)
				if err != nil {
					log.Printf("Unable to start a transaction: %v", err)
				}

				tm.Tx = tx
			}
		}
	}
}
