package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"pgcr-dataset-processor/pkg/config"
	"pgcr-dataset-processor/pkg/console"
	"pgcr-dataset-processor/pkg/dirscan"
	"pgcr-dataset-processor/pkg/postgres"
	"pgcr-dataset-processor/pkg/reader"
	"pgcr-dataset-processor/pkg/worker"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/lib/pq"
)

func main() {
	start := time.Now()
	done := make(chan struct{})
	config := config.ReadConfig()

	db, err := postgres.Connect(config.Datasource)
	if err != nil {
		log.Panicf("Unable to connect to postgres: %v", err)
	}

	finder := dirscan.FileFinder{
		Root: config.Directory,
	}
	filemap := finder.FindByExtension(".zst")
	numFiles := 0
	for _, entries := range filemap {
		numFiles += len(entries)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer stop()

	// Setup workers
	var counter atomic.Int64
	var wg sync.WaitGroup
	input := make(chan []byte, 200)

	transactionManager, err := postgres.NewTransactionManager(ctx, db, config.BatchSize)
	if err != nil {
		log.Panicf("Unable to create transaction manager: %v", err)
	}
	defer func() {
		if err := transactionManager.Close(ctx); err != nil {
			log.Printf("Error closing transaction manager: %v", err)
		}
	}()

	for range config.Workers {
		wg.Add(1)
		go func(wg *sync.WaitGroup) {
			defer wg.Done()

			worker := worker.NewDatasetWorker(input, transactionManager)
			worker.ProcessPgcr(ctx)
		}(&wg)
	}

	wg.Add(2)
	fileReader := reader.FileIngester{
		Files: filemap,
		Input: input,
	}
	go fileReader.IngestFiles(ctx, &wg)

	consoleOutput := console.Output{
		StartTime: start,
		Counter:   &counter,
		Done:      done,
	}
	go consoleOutput.DisplayOutput(ctx, &wg)

	wg.Wait()
	close(done)
}
