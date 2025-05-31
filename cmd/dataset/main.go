package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"pgcr-dataset-processor/pkg/config"
	"pgcr-dataset-processor/pkg/console"
	"pgcr-dataset-processor/pkg/dirscan"
	// "pgcr-dataset-processor/pkg/postgres"
	"pgcr-dataset-processor/pkg/reader"
	"pgcr-dataset-processor/pkg/worker"
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

	// db, err := postgres.Connect(config.Datasource)
	// if err != nil {
	// 	log.Panicf("Unable to connect to postgres: %v", err)
	// }

	finder := dirscan.FileFinder{
		Root: config.Directory,
	}
	filemap := finder.FindByExtension(".zst")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer stop()

	var wg sync.WaitGroup

	// Setup workers
	input := make(chan worker.PgcrLine, 200)

	// transactionManager, err := postgres.NewTransactionManager(ctx, db, config.BatchSize)
	// if err != nil {
	// 	log.Panicf("Unable to create transaction manager: %v", err)
	// }
	// defer func() {
	// 	if err := transactionManager.Close(ctx); err != nil {
	// 		log.Printf("Error closing transaction manager: %v", err)
	// 	}
	// }()

	for range config.Workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			worker := worker.NewMockWorker(input)
			worker.ProcessPgcr(ctx)
		}()
	}

	fileReader := reader.NewFileIngester(&filemap, input)
	wg.Add(1)
	go func() {
		defer wg.Done()
		fileReader.IngestFiles(ctx)
	}()

	wg.Add(1)
	consoleOutput := console.NewDisplayOutput(start, &filemap)
	go func() {
		defer wg.Done()
		consoleOutput.DisplayOutput(ctx)
	}()

	fmt.Print("Press Ctrl+C to end the process\n")
	<-ctx.Done()
	fmt.Print("Gracefully shutting down...\n")
	wg.Wait()
}
