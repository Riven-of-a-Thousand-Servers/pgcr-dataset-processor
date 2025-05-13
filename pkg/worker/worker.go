package worker

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"pgcr-dataset-processor/pkg/postgres"
	"pgcr-dataset-processor/pkg/types"
	"strconv"
)

type Worker struct {
	Inputs             <-chan []byte
	Progress           <-chan int
	TransactionManager *postgres.TransactionManager
}

func NewDatasetWorker(inputs chan []byte, transactionManager *postgres.TransactionManager) Worker {
	return Worker{
		Inputs:             inputs,
		TransactionManager: transactionManager,
	}
}

// This method will run until the channel of inputs is closed and has no values, or until context cancellation
func (w *Worker) ProcessPgcr(ctx context.Context) error {
	for {
		select {
		case input, ok := <-w.Inputs:
			if !ok {
				return nil
			}
			var pgcr types.PGCR
			err := json.Unmarshal(input, &pgcr)
			if err != nil {
				msg := fmt.Sprintf("Error unmarshaling pgcr: %v", err)
				log.Panicf(msg)
				return fmt.Errorf(msg)
			}

			compressed, err := compress(pgcr)
			if err != nil {
				msg := fmt.Sprintf("Error compressing pgcr [%s] using Gzip: %v", pgcr.ActivityDetails.InstanceID, err)
				log.Panicf(msg)
				return fmt.Errorf(msg)
			}

			instanceId, err := strconv.ParseInt(pgcr.ActivityDetails.InstanceID, 10, 64)
			if err != nil {
				return err
			}

			err = w.TransactionManager.AddPgcr(ctx, instanceId, compressed)
			if err != nil {
				return err
			}
		case <-ctx.Done():
			return nil
		}
	}
}

func compress(pgcr types.PGCR) ([]byte, error) {
	jsonData, err := json.Marshal(pgcr)
	if err != nil {
		return nil, err
	}

	var compressedBuffer bytes.Buffer
	gzipWriter := gzip.NewWriter(&compressedBuffer)

	_, err = gzipWriter.Write(jsonData)
	if err != nil {
		return nil, err
	}

	err = gzipWriter.Close()
	if err != nil {
		return nil, err
	}

	return compressedBuffer.Bytes(), err
}
