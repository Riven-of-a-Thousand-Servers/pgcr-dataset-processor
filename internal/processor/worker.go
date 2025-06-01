package processor

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"pgcr-dataset-processor/internal/db"
	"pgcr-dataset-processor/internal/parser"
)

type PgcrLine struct {
	Filepath   string
	Line       []byte
	LineNumber int
}

type Worker struct {
	Inputs             <-chan PgcrLine
	TransactionManager *db.TransactionManager
}

func NewMockWorker(inputs <-chan PgcrLine) Worker {
	return Worker{
		Inputs: inputs,
	}
}

func NewPgcrWorker(inputs chan PgcrLine, transactionManager *db.TransactionManager) Worker {
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
			var pgcr parser.PGCR
			err := json.Unmarshal(input.Line, &pgcr)

			if err != nil {
				msg := fmt.Sprintf("Error unmarshaling pgcr for filepath [%s] at line %d: %v", input.Filepath, input.LineNumber, err)
				log.Panicf(msg)
				return fmt.Errorf(msg)
			}

			// compressed, err := compress(pgcr)
			// if err != nil {
			// 	msg := fmt.Sprintf("Error compressing pgcr [%s] using Gzip: %v", pgcr.ActivityDetails.InstanceID, err)
			// 	log.Panicf(msg)
			// 	return fmt.Errorf(msg)
			// }

			// instanceId, err := pgcr.ActivityDetails.InstanceID.Int64()
			// if err != nil {
			// 	return err
			// }

			// err = w.TransactionManager.AddPgcr(ctx, instanceId, compressed)
			// if err != nil {
			// 	return err
			// }
		case <-ctx.Done():
			return nil
		}
	}
}

func compress(pgcr parser.PGCR) ([]byte, error) {
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
