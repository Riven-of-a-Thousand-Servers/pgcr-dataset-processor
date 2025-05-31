package reader

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"pgcr-dataset-processor/pkg/utils"
	"pgcr-dataset-processor/pkg/worker"

	"github.com/klauspost/compress/zstd"
)

type FileIngester struct {
	FileStatuses *utils.StatefulMap
	Input        chan worker.PgcrLine
}

func NewFileIngester(fileStatuses *utils.StatefulMap, input chan worker.PgcrLine) FileIngester {
	return FileIngester{
		Input:        input,
		FileStatuses: fileStatuses,
	}
}

// Reads all the files inside the Files map and attempts to place them on the input channel
func (fr *FileIngester) IngestFiles(ctx context.Context) error {
	for currentFile, entry := range fr.FileStatuses.Data {
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

		fr.FileStatuses.Mutex.Lock()

		status, ok := fr.FileStatuses.Data[currentFile]
		if ok {
			status.Started = true
			status.Progress = make(chan int64)
			fr.FileStatuses.Started <- currentFile
		}

		fr.FileStatuses.Mutex.Unlock()

		fileEntry, ok := fr.FileStatuses.Data[currentFile]
		if !ok {
			log.Panicf("No entry for file [%s] found in the stateful map", currentFile)
		}

		// Start channel for tracker progress
		count := 0
		for scanner.Scan() {
			select {
			case <-ctx.Done():
				close(fr.Input)
				return ctx.Err()
			default:
				fr.Input <- worker.PgcrLine{
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

	close(fr.Input)
	return nil
}
