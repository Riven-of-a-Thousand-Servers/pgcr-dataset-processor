package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"pgcr-dataset-processor/internal/config"
	mdb "pgcr-dataset-processor/internal/db" // mydb
	"pgcr-dataset-processor/internal/ingest"
	"pgcr-dataset-processor/internal/parser"
	"pgcr-dataset-processor/internal/processor"
	"pgcr-dataset-processor/internal/ui"
	"sync"
	"time"

	_ "github.com/lib/pq"
)

func main() {
	start := time.Now()
	file, err := os.ReadFile("config.yml")
	if err != nil {
		log.Panicf("Error reading config file: %v", err)
	}

	config, err := config.ReadConfig(file)
	if err != nil {
		log.Panicf("Error reading config: %v", err)
	}

	db, err := mdb.Connect(config.Datasource)
	if err != nil {
		log.Panicf("Unable to connect to postgres: %v", err)
	}

	finder := parser.FileFinder{
		Root: config.Directory,
	}
	filemap := finder.FindByExtension(".zst")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer stop()

	// Setup workers
	input := make(chan processor.PgcrLine, 200)
	var wg sync.WaitGroup

	wg.Add(1)
	txm, err := mdb.NewTransactionManager(ctx, db, &wg, config.BatchSize)
	if err != nil {
		log.Panicf("Unable to create transaction manager: %v", err)
	}

	for range config.Workers {
		wg.Add(1)
		processor.DoWork(ctx, &wg, input, txm)
	}

	wg.Add(1)
	ingest.StartIngesting(ctx, &wg, &filemap, input)

	wg.Add(1)
	ui.StartUi(ctx, &wg, start, &filemap)

	fmt.Print("Press Ctrl+C to end the process\n")
	<-ctx.Done()
	fmt.Print("Gracefully shutting down...\n")
	wg.Wait()
}
