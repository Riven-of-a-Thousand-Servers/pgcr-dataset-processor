package reader

import (
	"bufio"
	"context"
	"log"
	"os"
	"sync"

	"github.com/klauspost/compress/zstd"
)

type FileIngester struct {
	Files map[string][]string
	Input chan []byte
}

// Reads all the files inside the Files map and attempts to place them on the input channel
func (fr *FileIngester) IngestFiles(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			close(fr.Input)
			return
		default:
			for _, files := range fr.Files {
				for _, file := range files {
					file, err := os.Open(file)
					if err != nil {
						log.Panicf("Error opening file %v: %v", file, err)
					}

					bufReader := bufio.NewReader(file)
					decoder, err := zstd.NewReader(bufReader)
					if err != nil {
						log.Panicf("Something went wrong when creating a ZSTD reader: %v", err)
					}

					// 45MBs by default just in case we come across a checkpoint bot pgcr
					maxCapacity := 45 * 1024 * 1024
					buf := make([]byte, maxCapacity)
					scanner := bufio.NewScanner(decoder)
					scanner.Buffer(buf, maxCapacity)

					for scanner.Scan() {
						fr.Input <- scanner.Bytes()
					}
				}
			}
			close(fr.Input)
			return
		}
	}
}
