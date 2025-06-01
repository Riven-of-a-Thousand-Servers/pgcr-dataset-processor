package processor

import (
	"context"
	"os"
	"testing"
	"time"
)

var tests = map[string]struct {
	inputFile string
}{
	"empty pgcr": {
		inputFile: "./testdata/empty-pgcr.json",
	},
}

func Test_Process_Success(t *testing.T) {
	for test, file := range tests {
		t.Run(test, func(t *testing.T) {
			bytes, err := os.ReadFile(file.inputFile)
			if err != nil {
				t.Errorf("Unable to open file %s", file.inputFile)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			inputs := make(chan PgcrLine)
			w := Worker{
				Inputs: inputs,
			}

			done := make(chan error)
			go func() {
				done <- w.ProcessPgcr(ctx)
			}()

			inputs <- PgcrLine{
				Line:       bytes,
				LineNumber: 1,
			}
			close(inputs)

			err = w.ProcessPgcr(ctx)

			select {
			case err := <-done:
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
			case <-time.After(2 * time.Second):
				t.Error("Test timed out after 2 seconds - ProcessPgcr likely never exited")
			}
		})
	}
}
