package main

import (
	"context"
	"flag"
	"fmt"
	"io/fs"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"pgcr-dataset-processor/pkg/postgres"
	"pgcr-dataset-processor/pkg/reader"
	"pgcr-dataset-processor/pkg/types"
	"pgcr-dataset-processor/pkg/worker"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/lib/pq"
	"gopkg.in/yaml.v3"
)

const (
	datasetDirectory = "/Volumes/T7 Shield/"
	totalLines       = 10_000_000
	updateFreq       = 10_000
)

var (
	loader     = []rune{'|', '\\', '-', '/'}
	numWorkers = flag.Int64("workers", 100, "Number of workers to spin up")
)

func main() {
	done := make(chan int64)
	progress := make(chan int64, 1)

	start := time.Now()

	yamlFile, err := os.ReadFile("config.yml")
	if err != nil {
		log.Panicf("Unable to read config file: %v", err)
	}

	expandedYaml := os.ExpandEnv(string(yamlFile))

	var config types.Config
	err = yaml.Unmarshal([]byte(expandedYaml), &config)
	if err != nil {
		log.Panicf("Error marshaling datasource: %v", err)
	}

	db, err := postgres.Connect(config.Datasource)
	if err != nil {
		log.Panicf("Unable to connect to postgres: %v", err)
	}

	filemap := findFiles(datasetDirectory, ".zst")
	numFiles := 0
	for _, entries := range filemap {
		numFiles += len(entries)
	}

	// Setup workers
	var wg sync.WaitGroup
	input := make(chan []byte, 50)

	var counter atomic.Int32
	counter.Store(0)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer stop()

	transactionManager, err := postgres.NewTransactionManager(ctx, db, updateFreq)
	if err != nil {
		log.Panicf("Unable to create transaction manager: %v", err)
	}
	defer func() {
		if err := transactionManager.Close(ctx); err != nil {
			log.Printf("Error closing transaction manager: %v", err)
		}
	}()

	for range *numWorkers {
		wg.Add(1)
		go func(wg *sync.WaitGroup) {
			defer wg.Done()

			worker := worker.NewDatasetWorker(input, transactionManager)
			worker.ProcessPgcr(ctx)
		}(&wg)
	}

	fileReader := reader.FileReader{
		Files: filemap,
		Input: input,
	}

	// Reading file lines
	go fileReader.ReadFiles(ctx)

	go func() {
		var current int64
		var currentRune int
		for {
			select {
			case finally := <-done:
				fmt.Printf("\r✔ Done! Processed %d/%d lines (elapsed: %s)\n", finally, totalLines, time.Since(start).Truncate(time.Second))
				return
			case i := <-progress:
				current = i
			case <-ctx.Done():
				stop()
				return
			default:
				ellapsed := time.Since(start).Truncate(time.Second)
				progress := float64(current) / float64(totalLines) * 100
				fmt.Printf("\r%c Processing line %d/%d (%.2f%%) - elapsed: %s", loader[currentRune%len(loader)], current, totalLines, progress, ellapsed)
				currentRune++
				time.Sleep(100 * time.Millisecond)
			}
		}
	}()

	wg.Wait()
	done <- totalLines
}

func findFiles(root string, extension string) map[string][]string {
	paths := make(map[string][]string)
	filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			if os.IsPermission(err) {
				return filepath.SkipDir
			}
			return err
		}

		if d.IsDir() && strings.HasPrefix(d.Name(), ".") {
			return filepath.SkipDir
		}

		if !d.IsDir() && filepath.Ext(d.Name()) == extension {
			dir := filepath.Dir(path)
			paths[dir] = append(paths[dir], path)
		}
		return nil
	})
	return paths
}
