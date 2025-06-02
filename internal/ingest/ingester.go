package ingest

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"pgcr-dataset-processor/internal/parser"
	"pgcr-dataset-processor/internal/processor"
	"sync"

	"github.com/klauspost/compress/zstd"
)

type FileIngester struct {
	FileStatuses *parser.StatefulMap
	Input        chan processor.PgcrLine
	Ctx          context.Context
}

func StartIngesting(ctx context.Context, wg *sync.WaitGroup, fileStatuses *parser.StatefulMap, input chan processor.PgcrLine) {
	fi := FileIngester{
		Input:        input,
		Ctx:          ctx,
		FileStatuses: fileStatuses,
	}

	go func() {
		defer wg.Done()
		fi.ingestFiles()
	}()
}

// Reads all the files inside the Files map and attempts to place them on the input channel
func (fi *FileIngester) ingestFiles() error {
	for currentFile, entry := range fi.FileStatuses.Data {
		file, err := os.Open(entry.Path)
		if err != nil {
			log.Panicf("Error opening file %s: %v", file.Name(), err)
		}

		bufReader := bufio.NewReader(file)
		decoder, err := zstd.NewReader(bufReader)
		if err != nil {
			log.Panicf("Something went wrong when creating a ZSTD reader: %v", err)
			return err
		}

		// 45MBs by default just in case we come across a checkpoint bot pgcr
		maxCapacity := 46 * 1024 * 1024
		buf := make([]byte, maxCapacity)
		scanner := bufio.NewScanner(decoder)
		scanner.Buffer(buf, maxCapacity)

		status, ok := fi.FileStatuses.Data[currentFile]
		if ok {
			status.Started = true
			status.Progress = make(chan int64)
			fi.FileStatuses.Started <- currentFile
		}

		fileEntry, ok := fi.FileStatuses.Data[currentFile]
		if !ok {
			log.Panicf("No entry for file [%s] found in the stateful map", currentFile)
		}

		// Start channel for tracker progress
		count := 0
		for scanner.Scan() {
			select {
			case <-fi.Ctx.Done():
				close(fi.Input)
				return fi.Ctx.Err()
			default:
				fi.Input <- processor.PgcrLine{
					Filepath:   entry.Path,
					Line:       scanner.Bytes(),
					LineNumber: count,
				}

				fileEntry.Progress <- int64(1)
				count++
			}
		}

		close(fileEntry.Progress)
		err = file.Close()
		if err != nil {
			return fmt.Errorf("Error closing file: %s", currentFile)
		}
		decoder.Close()
	}

	close(fi.Input)
	return nil
}
